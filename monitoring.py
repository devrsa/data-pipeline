import json
import time
import psutil
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from loguru import logger
import redis
import sqlite3
import threading
from collections import defaultdict, deque
import pandas as pd

@dataclass
class PipelineMetrics:
    timestamp: datetime
    pipeline_name: str
    status: str
    rows_processed: int
    processing_time_seconds: float
    error_count: int
    memory_usage_mb: float
    cpu_usage_percent: float
    throughput_rows_per_second: float

@dataclass
class AlertRule:
    name: str
    metric: str
    threshold: float
    operator: str
    severity: str
    enabled: bool = True

class PipelineMonitor:
    def __init__(self, config_path: str = "config/monitoring_config.json"):
        self.config = self._load_config(config_path)
        self.metrics_store = MetricsStore()
        self.alert_manager = AlertManager()
        self.is_monitoring = False
        self.monitoring_thread = None
        
        logger.info("Pipeline Monitor initialized")
    
    def _load_config(self, config_path: str) -> Dict:
        try:
            with open(config_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            logger.warning("Monitoring config not found, using defaults")
            return self._get_default_config()
    
    def _get_default_config(self) -> Dict:
        return {
            "monitoring": {
                "interval_seconds": 30,
                "metrics_retention_days": 30,
                "enable_system_metrics": True,
                "enable_pipeline_metrics": True
            },
            "alerts": {
                "enabled": True,
                "notification_channels": ["slack", "email"],
                "slack_webhook": "https://hooks.slack.com/services/...",
                "email_recipients": ["admin@company.com"]
            },
            "storage": {
                "type": "sqlite",
                "path": "data/metrics.db",
                "redis_url": "redis://localhost:6379"
            }
        }
    
    def start_monitoring(self):
        if self.is_monitoring:
            logger.warning("Monitoring is already running")
            return
        
        self.is_monitoring = True
        self.monitoring_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitoring_thread.start()
        logger.info("Pipeline monitoring started")
    
    def stop_monitoring(self):
        self.is_monitoring = False
        if self.monitoring_thread:
            self.monitoring_thread.join(timeout=5)
        logger.info("Pipeline monitoring stopped")
    
    def _monitor_loop(self):
        while self.is_monitoring:
            try:
                # Collect system metrics
                if self.config["monitoring"]["enable_system_metrics"]:
                    system_metrics = self._collect_system_metrics()
                    self.metrics_store.store_metrics(system_metrics)
                
                # Check alert conditions
                if self.config["alerts"]["enabled"]:
                    self.alert_manager.check_alerts(self.metrics_store)
                
                time.sleep(self.config["monitoring"]["interval_seconds"])
                
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
                time.sleep(5)
    
    def _collect_system_metrics(self) -> Dict:
        return {
            "timestamp": datetime.now().isoformat(),
            "cpu_percent": psutil.cpu_percent(interval=1),
            "memory_percent": psutil.virtual_memory().percent,
            "disk_usage_percent": psutil.disk_usage('/').percent,
            "network_io": psutil.net_io_counters()._asdict(),
            "process_count": len(psutil.pids())
        }
    
    def record_pipeline_metrics(self, pipeline_name: str, status: str, 
                              rows_processed: int, processing_time: float,
                              error_count: int = 0):
        metrics = PipelineMetrics(
            timestamp=datetime.now(),
            pipeline_name=pipeline_name,
            status=status,
            rows_processed=rows_processed,
            processing_time_seconds=processing_time,
            error_count=error_count,
            memory_usage_mb=psutil.Process().memory_info().rss / 1024 / 1024,
            cpu_usage_percent=psutil.Process().cpu_percent(),
            throughput_rows_per_second=rows_processed / processing_time if processing_time > 0 else 0
        )
        
        self.metrics_store.store_pipeline_metrics(metrics)
        logger.info(f"Recorded metrics for pipeline: {pipeline_name}")
    
    def get_pipeline_health(self, pipeline_name: str, 
                          time_window_hours: int = 24) -> Dict:
        metrics = self.metrics_store.get_pipeline_metrics(
            pipeline_name, time_window_hours
        )
        
        if not metrics:
            return {"status": "no_data", "message": "No metrics found"}
        
        df = pd.DataFrame([asdict(m) for m in metrics])
        
        health_score = self._calculate_health_score(df)
        recent_errors = df[df['error_count'] > 0].shape[0]
        avg_throughput = df['throughput_rows_per_second'].mean()
        
        return {
            "status": "healthy" if health_score > 80 else "degraded" if health_score > 60 else "unhealthy",
            "health_score": health_score,
            "recent_errors": recent_errors,
            "avg_throughput": avg_throughput,
            "total_runs": len(df),
            "success_rate": (df['status'] == 'success').mean() * 100
        }
    
    def _calculate_health_score(self, df: pd.DataFrame) -> float:
        if df.empty:
            return 0.0
        
        scores = []
        
        # Success rate score
        success_rate = (df['status'] == 'success').mean()
        scores.append(success_rate * 40)
        
        # Error rate score
        error_rate = (df['error_count'] > 0).mean()
        scores.append((1 - error_rate) * 30)
        
        # Performance score
        avg_throughput = df['throughput_rows_per_second'].mean()
        expected_throughput = 1000  # Configurable threshold
        performance_score = min(avg_throughput / expected_throughput, 1.0)
        scores.append(performance_score * 30)
        
        return sum(scores)

class MetricsStore:
    def __init__(self):
        self.db_path = "data/metrics.db"
        self.redis_client = None
        self._init_storage()
    
    def _init_storage(self):
        # Initialize SQLite database
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS pipeline_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT,
                pipeline_name TEXT,
                status TEXT,
                rows_processed INTEGER,
                processing_time_seconds REAL,
                error_count INTEGER,
                memory_usage_mb REAL,
                cpu_usage_percent REAL,
                throughput_rows_per_second REAL
            )
        ''')
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS system_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT,
                cpu_percent REAL,
                memory_percent REAL,
                disk_usage_percent REAL,
                process_count INTEGER
            )
        ''')
        
        conn.commit()
        conn.close()
        
        # Initialize Redis for caching
        try:
            self.redis_client = redis.Redis(host='localhost', port=6379, db=0)
            self.redis_client.ping()
            logger.info("Redis connected for metrics caching")
        except:
            logger.warning("Redis not available, using only SQLite")
    
    def store_pipeline_metrics(self, metrics: PipelineMetrics):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO pipeline_metrics 
            (timestamp, pipeline_name, status, rows_processed, 
             processing_time_seconds, error_count, memory_usage_mb, 
             cpu_usage_percent, throughput_rows_per_second)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            metrics.timestamp.isoformat(),
            metrics.pipeline_name,
            metrics.status,
            metrics.rows_processed,
            metrics.processing_time_seconds,
            metrics.error_count,
            metrics.memory_usage_mb,
            metrics.cpu_usage_percent,
            metrics.throughput_rows_per_second
        ))
        
        conn.commit()
        conn.close()
        
        # Cache in Redis
        if self.redis_client:
            cache_key = f"pipeline_metrics:{metrics.pipeline_name}:{metrics.timestamp.isoformat()}"
            self.redis_client.setex(
                cache_key, 
                timedelta(hours=24), 
                json.dumps(asdict(metrics), default=str)
            )
    
    def store_metrics(self, metrics: Dict):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO system_metrics 
            (timestamp, cpu_percent, memory_percent, disk_usage_percent, process_count)
            VALUES (?, ?, ?, ?, ?)
        ''', (
            metrics["timestamp"],
            metrics["cpu_percent"],
            metrics["memory_percent"],
            metrics["disk_usage_percent"],
            metrics["process_count"]
        ))
        
        conn.commit()
        conn.close()
    
    def get_pipeline_metrics(self, pipeline_name: str, 
                           time_window_hours: int) -> List[PipelineMetrics]:
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cutoff_time = (datetime.now() - timedelta(hours=time_window_hours)).isoformat()
        
        cursor.execute('''
            SELECT timestamp, pipeline_name, status, rows_processed,
                   processing_time_seconds, error_count, memory_usage_mb,
                   cpu_usage_percent, throughput_rows_per_second
            FROM pipeline_metrics
            WHERE pipeline_name = ? AND timestamp > ?
            ORDER BY timestamp DESC
        ''', (pipeline_name, cutoff_time))
        
        rows = cursor.fetchall()
        conn.close()
        
        metrics = []
        for row in rows:
            metrics.append(PipelineMetrics(
                timestamp=datetime.fromisoformat(row[0]),
                pipeline_name=row[1],
                status=row[2],
                rows_processed=row[3],
                processing_time_seconds=row[4],
                error_count=row[5],
                memory_usage_mb=row[6],
                cpu_usage_percent=row[7],
                throughput_rows_per_second=row[8]
            ))
        
        return metrics

class AlertManager:
    def __init__(self):
        self.alert_rules = self._load_default_rules()
        self.alert_history = deque(maxlen=1000)
    
    def _load_default_rules(self) -> List[AlertRule]:
        return [
            AlertRule("High Error Rate", "error_rate", 0.1, ">", "high"),
            AlertRule("Low Throughput", "throughput", 100, "<", "medium"),
            AlertRule("High Memory Usage", "memory_percent", 80, ">", "high"),
            AlertRule("High CPU Usage", "cpu_percent", 90, ">", "medium"),
            AlertRule("Pipeline Failure", "status", "failed", "==", "critical")
        ]
    
    def check_alerts(self, metrics_store: MetricsStore):
        for rule in self.alert_rules:
            if not rule.enabled:
                continue
            
            try:
                alert_triggered = self._evaluate_rule(rule, metrics_store)
                if alert_triggered:
                    self._send_alert(rule, alert_triggered)
            except Exception as e:
                logger.error(f"Error evaluating alert rule {rule.name}: {e}")
    
    def _evaluate_rule(self, rule: AlertRule, 
                       metrics_store: MetricsStore) -> Optional[Dict]:
        # This is a simplified implementation
        # In practice, you'd query the metrics store and evaluate the condition
        
        if rule.metric == "error_rate":
            # Get recent error rate
            recent_metrics = metrics_store.get_pipeline_metrics("*", 1)
            if recent_metrics:
                total_errors = sum(m.error_count for m in recent_metrics)
                total_runs = len(recent_metrics)
                error_rate = total_errors / total_runs if total_runs > 0 else 0
                
                if self._compare_values(error_rate, rule.operator, rule.threshold):
                    return {
                        "metric_value": error_rate,
                        "threshold": rule.threshold,
                        "message": f"Error rate ({error_rate:.2%}) exceeds threshold ({rule.threshold:.2%})"
                    }
        
        return None
    
    def _compare_values(self, value: float, operator: str, threshold: float) -> bool:
        if operator == ">":
            return value > threshold
        elif operator == "<":
            return value < threshold
        elif operator == "==":
            return value == threshold
        elif operator == ">=":
            return value >= threshold
        elif operator == "<=":
            return value <= threshold
        return False
    
    def _send_alert(self, rule: AlertRule, alert_data: Dict):
        alert = {
            "rule_name": rule.name,
            "severity": rule.severity,
            "timestamp": datetime.now().isoformat(),
            "data": alert_data
        }
        
        self.alert_history.append(alert)
        
        # Send to notification channels
        self._send_slack_alert(alert)
        self._send_email_alert(alert)
        
        logger.warning(f"Alert triggered: {rule.name} - {alert_data['message']}")
    
    def _send_slack_alert(self, alert: Dict):
        try:
            webhook_url = "https://hooks.slack.com/services/YOUR/WEBHOOK/URL"
            
            payload = {
                "text": f"🚨 Pipeline Alert: {alert['rule_name']}",
                "attachments": [{
                    "color": "danger" if alert['severity'] == 'critical' else "warning",
                    "fields": [
                        {"title": "Severity", "value": alert['severity'], "short": True},
                        {"title": "Time", "value": alert['timestamp'], "short": True},
                        {"title": "Message", "value": alert['data']['message'], "short": False}
                    ]
                }]
            }
            
            response = requests.post(webhook_url, json=payload, timeout=10)
            if response.status_code == 200:
                logger.info("Slack alert sent successfully")
            else:
                logger.error(f"Failed to send Slack alert: {response.status_code}")
                
        except Exception as e:
            logger.error(f"Error sending Slack alert: {e}")
    
    def _send_email_alert(self, alert: Dict):
        # Email alert implementation would go here
        logger.info(f"Email alert would be sent for: {alert['rule_name']}")

class PerformanceAnalyzer:
    def __init__(self, metrics_store: MetricsStore):
        self.metrics_store = metrics_store
    
    def analyze_pipeline_performance(self, pipeline_name: str, 
                                   time_window_days: int = 7) -> Dict:
        metrics = self.metrics_store.get_pipeline_metrics(
            pipeline_name, time_window_days * 24
        )
        
        if not metrics:
            return {"status": "no_data"}
        
        df = pd.DataFrame([asdict(m) for m in metrics])
        
        analysis = {
            "pipeline_name": pipeline_name,
            "time_window_days": time_window_days,
            "total_runs": len(df),
            "success_rate": (df['status'] == 'success').mean() * 100,
            "avg_processing_time": df['processing_time_seconds'].mean(),
            "avg_throughput": df['throughput_rows_per_second'].mean(),
            "total_rows_processed": df['rows_processed'].sum(),
            "performance_trend": self._calculate_trend(df['throughput_rows_per_second']),
            "bottlenecks": self._identify_bottlenecks(df),
            "recommendations": self._generate_recommendations(df)
        }
        
        return analysis
    
    def _calculate_trend(self, series: pd.Series) -> str:
        if len(series) < 2:
            return "insufficient_data"
        
        # Simple linear trend calculation
        x = np.arange(len(series))
        slope = np.polyfit(x, series, 1)[0]
        
        if slope > 0.1:
            return "improving"
        elif slope < -0.1:
            return "degrading"
        else:
            return "stable"
    
    def _identify_bottlenecks(self, df: pd.DataFrame) -> List[str]:
        bottlenecks = []
        
        # High processing time
        if df['processing_time_seconds'].mean() > 60:
            bottlenecks.append("High processing time detected")
        
        # High memory usage
        if df['memory_usage_mb'].mean() > 1000:
            bottlenecks.append("High memory usage detected")
        
        # High error rate
        error_rate = (df['error_count'] > 0).mean()
        if error_rate > 0.05:
            bottlenecks.append("High error rate detected")
        
        return bottlenecks
    
    def _generate_recommendations(self, df: pd.DataFrame) -> List[str]:
        recommendations = []
        
        if df['processing_time_seconds'].mean() > 60:
            recommendations.append("Consider optimizing data processing logic")
        
        if df['memory_usage_mb'].mean() > 1000:
            recommendations.append("Consider implementing memory-efficient processing")
        
        if (df['status'] == 'success').mean() < 0.95:
            recommendations.append("Investigate and fix common failure patterns")
        
        return recommendations

if __name__ == "__main__":
    # Example usage
    monitor = PipelineMonitor()
    
    # Start monitoring
    monitor.start_monitoring()
    
    # Record some sample metrics
    monitor.record_pipeline_metrics(
        pipeline_name="customer_etl",
        status="success",
        rows_processed=10000,
        processing_time=45.5,
        error_count=0
    )
    
    # Get pipeline health
    health = monitor.get_pipeline_health("customer_etl")
    print(f"Pipeline health: {health}")
    
    # Analyze performance
    analyzer = PerformanceAnalyzer(monitor.metrics_store)
    performance = analyzer.analyze_pipeline_performance("customer_etl")
    print(f"Performance analysis: {performance}")
    
    time.sleep(5)
    monitor.stop_monitoring()