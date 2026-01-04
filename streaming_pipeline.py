import json
import time
from datetime import datetime
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Callable
from loguru import logger
import threading
import queue
from dataclasses import dataclass
import asyncio

@dataclass
class StreamMessage:
    topic: str
    key: str
    value: Dict
    timestamp: datetime
    partition: Optional[int] = None
    offset: Optional[int] = None

class KafkaStreamProcessor:
    def __init__(self, config_path: str = "config/kafka_config.json"):
        self.config = self._load_config(config_path)
        self.producer = None
        self.consumers = {}
        self.message_queue = queue.Queue()
        self.is_running = False
        self.processing_functions = {}
        
        logger.info("Kafka Stream Processor initialized")
    
    def _load_config(self, config_path: str) -> Dict:
        try:
            with open(config_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            logger.warning("Kafka config not found, using defaults")
            return self._get_default_config()
    
    def _get_default_config(self) -> Dict:
        return {
            "bootstrap_servers": ["localhost:9092"],
            "topics": {
                "user_events": "user-events",
                "system_metrics": "system-metrics",
                "processed_data": "processed-data"
            },
            "consumer_config": {
                "auto_offset_reset": "latest",
                "enable_auto_commit": True,
                "group_id": "data-pipeline-consumer",
                "value_deserializer": "json"
            },
            "producer_config": {
                "acks": "all",
                "retries": 3,
                "value_serializer": "json"
            }
        }
    
    def create_producer(self) -> KafkaProducer:
        try:
            producer = KafkaProducer(
                bootstrap_servers=self.config["bootstrap_servers"],
                acks=self.config["producer_config"]["acks"],
                retries=self.config["producer_config"]["retries"],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None
            )
            logger.info("Kafka producer created successfully")
            return producer
        except Exception as e:
            logger.error(f"Failed to create Kafka producer: {e}")
            raise
    
    def create_consumer(self, topic: str, group_id: str = None) -> KafkaConsumer:
        try:
            consumer_config = {
                "bootstrap_servers": self.config["bootstrap_servers"],
                "auto_offset_reset": self.config["consumer_config"]["auto_offset_reset"],
                "enable_auto_commit": self.config["consumer_config"]["enable_auto_commit"],
                "value_deserializer": lambda x: json.loads(x.decode('utf-8')),
                "key_deserializer": lambda x: x.decode('utf-8') if x else None
            }
            
            if group_id:
                consumer_config["group_id"] = group_id
            else:
                consumer_config["group_id"] = self.config["consumer_config"]["group_id"]
            
            consumer = KafkaConsumer(topic, **consumer_config)
            logger.info(f"Kafka consumer created for topic: {topic}")
            return consumer
        except Exception as e:
            logger.error(f"Failed to create Kafka consumer: {e}")
            raise
    
    def publish_message(self, topic: str, message: Dict, key: str = None) -> bool:
        if not self.producer:
            self.producer = self.create_producer()
        
        try:
            # Add metadata to message
            enriched_message = {
                **message,
                "timestamp": datetime.now().isoformat(),
                "source": "data-pipeline"
            }
            
            future = self.producer.send(
                topic=topic,
                value=enriched_message,
                key=key
            )
            
            # Block for a maximum of 1 second to confirm the send
            record_metadata = future.get(timeout=1)
            
            logger.info(f"Message sent to {topic} [{record_metadata.partition}] @ offset {record_metadata.offset}")
            return True
            
        except KafkaError as e:
            logger.error(f"Failed to send message to {topic}: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error sending message: {e}")
            return False
    
    def start_consumer(self, topic: str, processing_function: Callable = None, 
                      group_id: str = None) -> None:
        def consume_messages():
            consumer = self.create_consumer(topic, group_id)
            self.consumers[topic] = consumer
            
            logger.info(f"Starting consumer for topic: {topic}")
            
            try:
                for message in consumer:
                    stream_message = StreamMessage(
                        topic=message.topic,
                        key=message.key,
                        value=message.value,
                        timestamp=datetime.now(),
                        partition=message.partition,
                        offset=message.offset
                    )
                    
                    # Add to queue for batch processing
                    self.message_queue.put(stream_message)
                    
                    # Apply real-time processing function if provided
                    if processing_function:
                        try:
                            processing_function(stream_message)
                        except Exception as e:
                            logger.error(f"Error in processing function: {e}")
                    
                    logger.debug(f"Consumed message from {topic}: {message.key}")
                    
            except Exception as e:
                logger.error(f"Consumer error for topic {topic}: {e}")
            finally:
                consumer.close()
        
        # Start consumer in a separate thread
        consumer_thread = threading.Thread(target=consume_messages, daemon=True)
        consumer_thread.start()
        self.is_running = True
    
    def register_processing_function(self, event_type: str, function: Callable):
        self.processing_functions[event_type] = function
        logger.info(f"Registered processing function for event type: {event_type}")
    
    def process_stream_batch(self, batch_size: int = 100, 
                           timeout: float = 5.0) -> List[StreamMessage]:
        messages = []
        start_time = time.time()
        
        while len(messages) < batch_size and (time.time() - start_time) < timeout:
            try:
                message = self.message_queue.get(timeout=0.1)
                messages.append(message)
            except queue.Empty:
                continue
        
        if messages:
            logger.info(f"Processed batch of {len(messages)} messages")
        
        return messages
    
    def aggregate_stream_data(self, messages: List[StreamMessage], 
                            aggregation_window: int = 60) -> Dict:
        if not messages:
            return {}
        
        # Convert to DataFrame for easier aggregation
        df_data = []
        for msg in messages:
            msg_data = msg.value.copy()
            msg_data['topic'] = msg.topic
            msg_data['key'] = msg.key
            msg_data['timestamp'] = msg.timestamp
            df_data.append(msg_data)
        
        df = pd.DataFrame(df_data)
        
        # Perform aggregations
        aggregations = {
            "message_count": len(messages),
            "topics": df['topic'].value_counts().to_dict(),
            "time_window": aggregation_window,
            "start_time": min(msg.timestamp for msg in messages).isoformat(),
            "end_time": max(msg.timestamp for msg in messages).isoformat()
        }
        
        # Add numeric aggregations if available
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        if len(numeric_cols) > 0:
            aggregations["numeric_summary"] = df[numeric_cols].describe().to_dict()
        
        return aggregations
    
    def create_stream_enrichment(self, topic: str, enrichment_rules: Dict) -> None:
        def enrich_message(message: StreamMessage):
            try:
                enriched_data = message.value.copy()
                
                # Apply enrichment rules
                for field, rule in enrichment_rules.items():
                    if rule["type"] == "timestamp":
                        enriched_data[field] = datetime.now().isoformat()
                    elif rule["type"] == "computed":
                        # Simple computed field example
                        if "value" in enriched_data:
                            enriched_data[field] = enriched_data["value"] * rule.get("multiplier", 1)
                    elif rule["type"] == "lookup":
                        # This would typically involve a database lookup
                        enriched_data[field] = f"lookup_{enriched_data.get('id', 'unknown')}"
                
                # Publish enriched message
                enriched_topic = f"{topic}_enriched"
                self.publish_message(enriched_topic, enriched_data, message.key)
                
            except Exception as e:
                logger.error(f"Error enriching message: {e}")
        
        self.register_processing_function(f"enrich_{topic}", enrich_message)
        logger.info(f"Created stream enrichment for topic: {topic}")
    
    def create_stream_filter(self, topic: str, filter_conditions: Dict) -> None:
        def filter_message(message: StreamMessage):
            try:
                data = message.value
                
                # Apply filter conditions
 passes_filter = True
                for field, condition in filter_conditions.items():
                    if field not in data:
                        passes_filter = False
                        break
                    
                    value = data[field]
                    if condition["type"] == "equals":
                        if value != condition["value"]:
                            passes_filter = False
                            break
                    elif condition["type"] == "range":
                        if not (condition["min"] <= value <= condition["max"]):
                            passes_filter = False
                            break
                    elif condition["type"] == "contains":
                        if condition["value"] not in str(value):
                            passes_filter = False
                            break
                
                if passes_filter:
                    # Publish to filtered topic
                    filtered_topic = f"{topic}_filtered"
                    self.publish_message(filtered_topic, data, message.key)
                
            except Exception as e:
                logger.error(f"Error filtering message: {e}")
        
        self.register_processing_function(f"filter_{topic}", filter_message)
        logger.info(f"Created stream filter for topic: {topic}")
    
    def close(self):
        if self.producer:
            self.producer.flush()
            self.producer.close()
        
        for consumer in self.consumers.values():
            consumer.close()
        
        self.is_running = False
        logger.info("Kafka Stream Processor closed")

class StreamDataPipeline:
    def __init__(self):
        self.processor = KafkaStreamProcessor()
        self.aggregated_data = {}
        
    def setup_pipeline(self):
        # Define enrichment rules
        user_enrichment_rules = {
            "processed_at": {"type": "timestamp"},
            "user_score": {"type": "computed", "multiplier": 1.5},
            "user_segment": {"type": "lookup"}
        }
        
        # Define filter conditions
        user_filter_conditions = {
            "age": {"type": "range", "min": 18, "max": 100},
            "status": {"type": "equals", "value": "active"}
        }
        
        # Set up stream processing
        self.processor.create_stream_enrichment("user_events", user_enrichment_rules)
        self.processor.create_stream_filter("user_events", user_filter_conditions)
        
        logger.info("Stream data pipeline setup complete")
    
    def run_pipeline(self):
        self.setup_pipeline()
        
        # Start consumers for different topics
        topics = ["user_events", "system_metrics"]
        
        for topic in topics:
            self.processor.start_consumer(topic)
        
        try:
            while self.processor.is_running:
                # Process messages in batches
                messages = self.processor.process_stream_batch(batch_size=50)
                
                if messages:
                    # Aggregate data
                    aggregated = self.processor.aggregate_stream_data(messages)
                    self.aggregated_data[datetime.now().isoformat()] = aggregated
                    
                    # Publish aggregated data
                    self.processor.publish_message(
                        "aggregated_metrics",
                        aggregated,
                        key="aggregation"
                    )
                
                time.sleep(1)
                
        except KeyboardInterrupt:
            logger.info("Pipeline stopped by user")
        finally:
            self.processor.close()

if __name__ == "__main__":
    # Example usage
    pipeline = StreamDataPipeline()
    
    # Publish some test messages
    test_messages = [
        {"user_id": 1, "name": "Alice", "age": 25, "status": "active", "value": 100},
        {"user_id": 2, "name": "Bob", "age": 30, "status": "inactive", "value": 150},
        {"user_id": 3, "name": "Charlie", "age": 35, "status": "active", "value": 200}
    ]
    
    for i, msg in enumerate(test_messages):
        pipeline.processor.publish_message("user_events", msg, key=f"user_{msg['user_id']}")
        time.sleep(0.1)
    
    print("Test messages published to Kafka")
    print("Run the pipeline to start processing streams")