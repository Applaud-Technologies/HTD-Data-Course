# Advanced Error Handling & Monitoring Patterns for Data Engineering

## Introduction

This guide covers enterprise-level error handling and monitoring patterns for production data pipelines. These techniques go beyond basic try/catch blocks to create robust, observable, and maintainable data engineering systems that can handle failures gracefully and provide actionable insights for operations teams.

## Comprehensive Logging Architecture

### Structured Logging with Context

```python
import logging
import json
import time
import traceback
from datetime import datetime
from typing import Dict, Any, Optional
from contextlib import contextmanager
import psutil
import os

class StructuredLogger:
    """
    Enterprise-grade structured logging for data pipelines
    Provides consistent, searchable log format for monitoring systems
    """
    
    def __init__(self, name: str, log_level: str = "INFO"):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(getattr(logging, log_level.upper()))
        
        # Remove existing handlers to avoid duplicates
        for handler in self.logger.handlers[:]:
            self.logger.removeHandler(handler)
        
        # Create structured formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        # File handler for persistent logs
        file_handler = logging.FileHandler(f'etl_pipeline_{datetime.now().strftime("%Y%m%d")}.log')
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)
        
        # Console handler for development
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)
    
    def log_structured(self, level: str, message: str, context: Dict[str, Any] = None):
        """
        Log structured data that can be easily parsed by monitoring systems
        """
        log_entry = {
            'timestamp': datetime.utcnow().isoformat(),
            'level': level.upper(),
            'message': message,
            'context': context or {},
            'system_info': {
                'process_id': os.getpid(),
                'memory_usage_mb': psutil.Process().memory_info().rss / 1024 / 1024,
                'cpu_percent': psutil.cpu_percent()
            }
        }
        
        # Log as JSON for parsing by log aggregation systems
        json_log = json.dumps(log_entry, default=str)
        getattr(self.logger, level.lower())(json_log)
        
        return log_entry
    
    def info(self, message: str, **context):
        return self.log_structured('INFO', message, context)
    
    def warning(self, message: str, **context):
        return self.log_structured('WARNING', message, context)
    
    def error(self, message: str, **context):
        return self.log_structured('ERROR', message, context)
    
    def critical(self, message: str, **context):
        return self.log_structured('CRITICAL', message, context)

# Global logger instance
logger = StructuredLogger('data_pipeline')
```

### Performance and Resource Monitoring

```python
class ResourceMonitor:
    """
    Advanced resource monitoring for data pipeline performance
    Tracks memory, CPU, I/O, and custom metrics
    """
    
    def __init__(self):
        self.metrics = {}
        self.start_time = None
        self.checkpoints = []
    
    @contextmanager
    def monitor_operation(self, operation_name: str):
        """
        Context manager to monitor resource usage during operations
        """
        start_time = time.time()
        start_memory = psutil.Process().memory_info().rss / 1024 / 1024
        start_cpu = psutil.cpu_percent()
        
        logger.info(f"Starting operation: {operation_name}", 
                   operation=operation_name,
                   start_memory_mb=start_memory,
                   start_cpu_percent=start_cpu)
        
        try:
            yield self
        except Exception as e:
            logger.error(f"Operation failed: {operation_name}",
                        operation=operation_name,
                        error=str(e),
                        traceback=traceback.format_exc())
            raise
        finally:
            end_time = time.time()
            end_memory = psutil.Process().memory_info().rss / 1024 / 1024
            end_cpu = psutil.cpu_percent()
            duration = end_time - start_time
            
            self.metrics[operation_name] = {
                'duration_seconds': duration,
                'memory_change_mb': end_memory - start_memory,
                'peak_memory_mb': end_memory,
                'avg_cpu_percent': (start_cpu + end_cpu) / 2
            }
            
            logger.info(f"Completed operation: {operation_name}",
                       operation=operation_name,
                       duration_seconds=duration,
                       memory_change_mb=end_memory - start_memory,
                       peak_memory_mb=end_memory)
    
    def add_checkpoint(self, name: str, custom_metrics: Dict[str, Any] = None):
        """Add performance checkpoint during long-running operations"""
        checkpoint = {
            'name': name,
            'timestamp': datetime.utcnow().isoformat(),
            'memory_mb': psutil.Process().memory_info().rss / 1024 / 1024,
            'cpu_percent': psutil.cpu_percent(),
            'custom_metrics': custom_metrics or {}
        }
        
        self.checkpoints.append(checkpoint)
        logger.info(f"Checkpoint: {name}", **checkpoint)
        
        return checkpoint
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """Generate comprehensive performance summary"""
        return {
            'total_operations': len(self.metrics),
            'operation_metrics': self.metrics,
            'checkpoints': self.checkpoints,
            'summary_stats': {
                'total_duration': sum(m.get('duration_seconds', 0) for m in self.metrics.values()),
                'peak_memory_usage': max((m.get('peak_memory_mb', 0) for m in self.metrics.values()), default=0),
                'total_memory_allocated': sum(m.get('memory_change_mb', 0) for m in self.metrics.values())
            }
        }

# Example usage
monitor = ResourceMonitor()
```

## Circuit Breaker Pattern for Data Sources

```python
from enum import Enum
from threading import Lock
import time

class CircuitState(Enum):
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Failing, blocking requests
    HALF_OPEN = "half_open"  # Testing if service recovered

class CircuitBreaker:
    """
    Circuit breaker pattern for data source connections
    Prevents cascade failures and provides graceful degradation
    """
    
    def __init__(self, failure_threshold: int = 5, timeout: int = 60, expected_exception: Exception = Exception):
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.expected_exception = expected_exception
        
        self.failure_count = 0
        self.last_failure_time = None
        self.state = CircuitState.CLOSED
        self.lock = Lock()
    
    def __call__(self, func):
        def wrapper(*args, **kwargs):
            return self._call_with_circuit_breaker(func, *args, **kwargs)
        return wrapper
    
    def _call_with_circuit_breaker(self, func, *args, **kwargs):
        with self.lock:
            if self.state == CircuitState.OPEN:
                if self._should_attempt_reset():
                    self.state = CircuitState.HALF_OPEN
                    logger.info("Circuit breaker: attempting reset",
                               circuit_state=self.state.value,
                               function=func.__name__)
                else:
                    logger.warning("Circuit breaker: request blocked",
                                  circuit_state=self.state.value,
                                  function=func.__name__)
                    raise Exception("Circuit breaker is OPEN - service unavailable")
        
        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        
        except self.expected_exception as e:
            self._on_failure()
            logger.error("Circuit breaker: function failed",
                        function=func.__name__,
                        error=str(e),
                        failure_count=self.failure_count,
                        circuit_state=self.state.value)
            raise
    
    def _should_attempt_reset(self) -> bool:
        return (self.last_failure_time and 
                time.time() - self.last_failure_time >= self.timeout)
    
    def _on_success(self):
        with self.lock:
            self.failure_count = 0
            self.state = CircuitState.CLOSED
            logger.info("Circuit breaker: reset to CLOSED",
                       circuit_state=self.state.value)
    
    def _on_failure(self):
        with self.lock:
            self.failure_count += 1
            self.last_failure_time = time.time()
            
            if self.failure_count >= self.failure_threshold:
                self.state = CircuitState.OPEN
                logger.critical("Circuit breaker: opened due to failures",
                               failure_count=self.failure_count,
                               circuit_state=self.state.value)

# Example usage with database connections
@CircuitBreaker(failure_threshold=3, timeout=30)
def connect_to_database():
    # Database connection logic
    pass

@CircuitBreaker(failure_threshold=5, timeout=60)
def fetch_from_api():
    # API call logic
    pass
```

## Advanced Data Quality Monitoring

```python
import pandas as pd
import numpy as np
from typing import List, Dict, Callable, Tuple
from dataclasses import dataclass
from abc import ABC, abstractmethod

@dataclass
class DataQualityRule:
    """Data quality rule definition"""
    name: str
    description: str
    severity: str  # 'error', 'warning', 'info'
    threshold: float = None

class DataQualityCheck(ABC):
    """Abstract base class for data quality checks"""
    
    @abstractmethod
    def check(self, df: pd.DataFrame) -> Tuple[bool, Dict[str, Any]]:
        """
        Execute the quality check
        Returns: (passed: bool, details: Dict)
        """
        pass

class CompletenessCheck(DataQualityCheck):
    """Check for missing data completeness"""
    
    def __init__(self, rule: DataQualityRule, columns: List[str]):
        self.rule = rule
        self.columns = columns
    
    def check(self, df: pd.DataFrame) -> Tuple[bool, Dict[str, Any]]:
        results = {}
        overall_passed = True
        
        for column in self.columns:
            if column not in df.columns:
                results[column] = {
                    'status': 'error',
                    'message': f'Column {column} not found in dataset'
                }
                overall_passed = False
                continue
            
            missing_count = df[column].isnull().sum()
            missing_percentage = (missing_count / len(df)) * 100
            
            passed = missing_percentage <= (self.rule.threshold or 0)
            
            results[column] = {
                'missing_count': missing_count,
                'missing_percentage': round(missing_percentage, 2),
                'total_rows': len(df),
                'passed': passed,
                'threshold': self.rule.threshold
            }
            
            if not passed:
                overall_passed = False
        
        return overall_passed, results

class UniquenessCheck(DataQualityCheck):
    """Check for uniqueness constraints"""
    
    def __init__(self, rule: DataQualityRule, columns: List[str]):
        self.rule = rule
        self.columns = columns
    
    def check(self, df: pd.DataFrame) -> Tuple[bool, Dict[str, Any]]:
        results = {}
        overall_passed = True
        
        for column in self.columns:
            if column not in df.columns:
                results[column] = {
                    'status': 'error',
                    'message': f'Column {column} not found in dataset'
                }
                overall_passed = False
                continue
            
            total_values = df[column].count()  # Non-null values
            unique_values = df[column].nunique()
            duplicate_count = total_values - unique_values
            duplicate_percentage = (duplicate_count / total_values) * 100 if total_values > 0 else 0
            
            passed = duplicate_percentage <= (self.rule.threshold or 0)
            
            results[column] = {
                'total_values': total_values,
                'unique_values': unique_values,
                'duplicate_count': duplicate_count,
                'duplicate_percentage': round(duplicate_percentage, 2),
                'passed': passed,
                'threshold': self.rule.threshold
            }
            
            if not passed:
                overall_passed = False
        
        return overall_passed, results

class RangeCheck(DataQualityCheck):
    """Check for value ranges"""
    
    def __init__(self, rule: DataQualityRule, column: str, min_value: float = None, max_value: float = None):
        self.rule = rule
        self.column = column
        self.min_value = min_value
        self.max_value = max_value
    
    def check(self, df: pd.DataFrame) -> Tuple[bool, Dict[str, Any]]:
        if self.column not in df.columns:
            return False, {
                'status': 'error',
                'message': f'Column {self.column} not found in dataset'
            }
        
        column_data = df[self.column].dropna()
        out_of_range_count = 0
        
        if self.min_value is not None:
            out_of_range_count += (column_data < self.min_value).sum()
        
        if self.max_value is not None:
            out_of_range_count += (column_data > self.max_value).sum()
        
        out_of_range_percentage = (out_of_range_count / len(column_data)) * 100 if len(column_data) > 0 else 0
        passed = out_of_range_percentage <= (self.rule.threshold or 0)
        
        results = {
            'total_values': len(column_data),
            'out_of_range_count': out_of_range_count,
            'out_of_range_percentage': round(out_of_range_percentage, 2),
            'min_value': self.min_value,
            'max_value': self.max_value,
            'actual_min': column_data.min() if len(column_data) > 0 else None,
            'actual_max': column_data.max() if len(column_data) > 0 else None,
            'passed': passed,
            'threshold': self.rule.threshold
        }
        
        return passed, results

class DataQualityMonitor:
    """
    Comprehensive data quality monitoring system
    Orchestrates multiple quality checks and generates reports
    """
    
    def __init__(self):
        self.checks = []
        self.results_history = []
    
    def add_check(self, check: DataQualityCheck):
        """Add a data quality check"""
        self.checks.append(check)
    
    def run_checks(self, df: pd.DataFrame, dataset_name: str = "Unknown") -> Dict[str, Any]:
        """Run all configured quality checks"""
        start_time = time.time()
        
        logger.info("Starting data quality assessment",
                   dataset_name=dataset_name,
                   total_checks=len(self.checks),
                   dataset_shape=df.shape)
        
        check_results = []
        overall_status = 'passed'
        
        for i, check in enumerate(self.checks):
            check_start = time.time()
            
            try:
                passed, details = check.check(df)
                
                check_result = {
                    'check_name': check.rule.name,
                    'check_type': type(check).__name__,
                    'description': check.rule.description,
                    'severity': check.rule.severity,
                    'passed': passed,
                    'details': details,
                    'execution_time': time.time() - check_start
                }
                
                check_results.append(check_result)
                
                if not passed and check.rule.severity == 'error':
                    overall_status = 'failed'
                elif not passed and check.rule.severity == 'warning' and overall_status != 'failed':
                    overall_status = 'warning'
                
                logger.info(f"Quality check completed: {check.rule.name}",
                           check_name=check.rule.name,
                           passed=passed,
                           severity=check.rule.severity,
                           execution_time=time.time() - check_start)
                
            except Exception as e:
                error_result = {
                    'check_name': check.rule.name,
                    'check_type': type(check).__name__,
                    'description': check.rule.description,
                    'severity': 'error',
                    'passed': False,
                    'error': str(e),
                    'execution_time': time.time() - check_start
                }
                
                check_results.append(error_result)
                overall_status = 'failed'
                
                logger.error(f"Quality check failed with error: {check.rule.name}",
                            check_name=check.rule.name,
                            error=str(e))
        
        # Generate summary report
        total_time = time.time() - start_time
        
        summary = {
            'dataset_name': dataset_name,
            'timestamp': datetime.utcnow().isoformat(),
            'overall_status': overall_status,
            'total_checks': len(self.checks),
            'passed_checks': sum(1 for r in check_results if r['passed']),
            'failed_checks': sum(1 for r in check_results if not r['passed']),
            'execution_time': total_time,
            'dataset_info': {
                'rows': len(df),
                'columns': len(df.columns),
                'memory_usage_mb': df.memory_usage().sum() / 1024 / 1024
            },
            'check_results': check_results
        }
        
        # Store in history
        self.results_history.append(summary)
        
        logger.info("Data quality assessment completed",
                   dataset_name=dataset_name,
                   overall_status=overall_status,
                   total_time=total_time,
                   passed_checks=summary['passed_checks'],
                   failed_checks=summary['failed_checks'])
        
        return summary
    
    def generate_quality_report(self, summary: Dict[str, Any]) -> str:
        """Generate human-readable data quality report"""
        
        report = f"""
DATA QUALITY REPORT
==================
Dataset: {summary['dataset_name']}
Timestamp: {summary['timestamp']}
Overall Status: {summary['overall_status'].upper()}

Dataset Information:
- Rows: {summary['dataset_info']['rows']:,}
- Columns: {summary['dataset_info']['columns']}
- Memory Usage: {summary['dataset_info']['memory_usage_mb']:.2f} MB

Check Summary:
- Total Checks: {summary['total_checks']}
- Passed: {summary['passed_checks']}
- Failed: {summary['failed_checks']}
- Execution Time: {summary['execution_time']:.2f} seconds

Detailed Results:
"""
        
        for result in summary['check_results']:
            status_icon = "✓" if result['passed'] else "✗"
            report += f"\n{status_icon} {result['check_name']} ({result['severity'].upper()})\n"
            report += f"   Description: {result['description']}\n"
            
            if 'error' in result:
                report += f"   Error: {result['error']}\n"
            elif isinstance(result['details'], dict):
                for key, value in result['details'].items():
                    if isinstance(value, dict):
                        report += f"   {key}:\n"
                        for sub_key, sub_value in value.items():
                            report += f"     {sub_key}: {sub_value}\n"
                    else:
                        report += f"   {key}: {value}\n"
        
        return report

# Example usage
def setup_quality_monitoring():
    """Setup comprehensive quality monitoring"""
    
    monitor = DataQualityMonitor()
    
    # Add completeness checks
    completeness_rule = DataQualityRule(
        name="Required Fields Completeness",
        description="Check that required fields have minimal missing values",
        severity="error",
        threshold=5.0  # Max 5% missing values allowed
    )
    monitor.add_check(CompletenessCheck(completeness_rule, ['customer_id', 'order_date', 'amount']))
    
    # Add uniqueness checks
    uniqueness_rule = DataQualityRule(
        name="Primary Key Uniqueness",
        description="Check that primary key fields are unique",
        severity="error",
        threshold=0.0  # No duplicates allowed
    )
    monitor.add_check(UniquenessCheck(uniqueness_rule, ['order_id']))
    
    # Add range checks
    amount_range_rule = DataQualityRule(
        name="Amount Range Validation",
        description="Check that amounts are within reasonable business ranges",
        severity="warning",
        threshold=1.0  # Max 1% out of range allowed
    )
    monitor.add_check(RangeCheck(amount_range_rule, 'amount', min_value=0, max_value=10000))
    
    return monitor

# Example usage
quality_monitor = setup_quality_monitoring()
```

## Alerting and Notification Systems

```python
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import List
import requests
import json

class AlertManager:
    """
    Enterprise alerting system for data pipeline monitoring
    Supports email, Slack, and webhook notifications
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.alert_history = []
    
    def send_alert(self, alert_type: str, title: str, message: str, 
                   severity: str = 'warning', context: Dict[str, Any] = None):
        """
        Send alert through configured channels based on severity
        """
        alert = {
            'timestamp': datetime.utcnow().isoformat(),
            'alert_type': alert_type,
            'title': title,
            'message': message,
            'severity': severity,
            'context': context or {}
        }
        
        self.alert_history.append(alert)
        
        # Route alerts based on severity
        if severity in ['critical', 'error']:
            self._send_email_alert(alert)
            self._send_slack_alert(alert)
        elif severity == 'warning':
            self._send_slack_alert(alert)
        
        # Always log alerts
        logger.log_structured(severity.upper(), f"ALERT: {title}", alert)
    
    def _send_email_alert(self, alert: Dict[str, Any]):
        """Send email alert for critical issues"""
        if 'email' not in self.config:
            return
        
        try:
            email_config = self.config['email']
            
            msg = MIMEMultipart()
            msg['From'] = email_config['from_address']
            msg['To'] = ', '.join(email_config['to_addresses'])
            msg['Subject'] = f"[{alert['severity'].upper()}] Data Pipeline Alert: {alert['title']}"
            
            body = f"""
Data Pipeline Alert

Alert Type: {alert['alert_type']}
Severity: {alert['severity'].upper()}
Timestamp: {alert['timestamp']}

Title: {alert['title']}

Message:
{alert['message']}

Context:
{json.dumps(alert['context'], indent=2, default=str)}

---
This is an automated alert from the data pipeline monitoring system.
"""
            
            msg.attach(MIMEText(body, 'plain'))
            
            server = smtplib.SMTP(email_config['smtp_server'], email_config['smtp_port'])
            if email_config.get('use_tls'):
                server.starttls()
            if email_config.get('username'):
                server.login(email_config['username'], email_config['password'])
            
            server.send_message(msg)
            server.quit()
            
            logger.info("Email alert sent successfully", 
                       alert_type=alert['alert_type'],
                       recipients=email_config['to_addresses'])
            
        except Exception as e:
            logger.error("Failed to send email alert", 
                        error=str(e),
                        alert_type=alert['alert_type'])
    
    def _send_slack_alert(self, alert: Dict[str, Any]):
        """Send Slack alert for warnings and errors"""
        if 'slack' not in self.config:
            return
        
        try:
            slack_config = self.config['slack']
            
            # Color coding for Slack
            color_map = {
                'info': '#36a64f',      # Green
                'warning': '#ff9500',   # Orange
                'error': '#ff0000',     # Red
                'critical': '#800080'   # Purple
            }
            
            slack_message = {
                'channel': slack_config['channel'],
                'username': 'Data Pipeline Monitor',
                'icon_emoji': ':warning:',
                'attachments': [{
                    'color': color_map.get(alert['severity'], '#cccccc'),
                    'title': f"{alert['severity'].upper()}: {alert['title']}",
                    'text': alert['message'],
                    'fields': [
                        {
                            'title': 'Alert Type',
                            'value': alert['alert_type'],
                            'short': True
                        },
                        {
                            'title': 'Timestamp',
                            'value': alert['timestamp'],
                            'short': True
                        }
                    ],
                    'footer': 'Data Pipeline Monitor',
                    'ts': int(time.time())
                }]
            }
            
            response = requests.post(
                slack_config['webhook_url'],
                data=json.dumps(slack_message),
                headers={'Content-Type': 'application/json'}
            )
            
            if response.status_code == 200:
                logger.info("Slack alert sent successfully",
                           alert_type=alert['alert_type'],
                           channel=slack_config['channel'])
            else:
                logger.error("Failed to send Slack alert",
                            status_code=response.status_code,
                            response=response.text)
                
        except Exception as e:
            logger.error("Failed to send Slack alert",
                        error=str(e),
                        alert_type=alert['alert_type'])
    
    def get_alert_summary(self, hours: int = 24) -> Dict[str, Any]:
        """Get summary of alerts from the last N hours"""
        cutoff_time = datetime.utcnow() - timedelta(hours=hours)
        cutoff_iso = cutoff_time.isoformat()
        
        recent_alerts = [
            alert for alert in self.alert_history 
            if alert['timestamp'] > cutoff_iso
        ]
        
        summary = {
            'time_period_hours': hours,
            'total_alerts': len(recent_alerts),
            'by_severity': {},
            'by_type': {},
            'recent_alerts': recent_alerts[-10:]  # Last 10 alerts
        }
        
        for alert in recent_alerts:
            # Count by severity
            severity = alert['severity']
            summary['by_severity'][severity] = summary['by_severity'].get(severity, 0) + 1
            
            # Count by type
            alert_type = alert['alert_type']
            summary['by_type'][alert_type] = summary['by_type'].get(alert_type, 0) + 1
        
        return summary

# Example configuration and usage
alert_config = {
    'email': {
        'smtp_server': 'smtp.company.com',
        'smtp_port': 587,
        'use_tls': True,
        'from_address': 'data-pipeline@company.com',
        'to_addresses': ['data-team@company.com', 'ops-team@company.com'],
        'username': 'data-pipeline@company.com',
        'password': 'secure_password'
    },
    'slack': {
        'webhook_url': 'https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK',
        'channel': '#data-alerts'
    }
}

alert_manager = AlertManager(alert_config)
```

## Comprehensive Pipeline Health Monitoring

```python
class PipelineHealthMonitor:
    """
    Comprehensive health monitoring for data pipelines
    Tracks SLAs, performance trends, and system health
    """
    
    def __init__(self, alert_manager: AlertManager):
        self.alert_manager = alert_manager
        self.health_metrics = {
            'pipeline_runs': [],
            'data_quality_scores': [],
            'performance_metrics': [],
            'error_rates': [],
            'sla_compliance': []
        }
        self.sla_thresholds = {
            'max_execution_time_minutes': 60,
            'min_success_rate_percent': 95,
            'max_error_rate_percent': 5,
            'min_data_quality_score': 0.95
        }
    
    def record_pipeline_execution(self, execution_data: Dict[str, Any]):
        """Record pipeline execution data for monitoring"""
        
        execution_record = {
            'timestamp': datetime.utcnow().isoformat(),
            'pipeline_name': execution_data.get('pipeline_name', 'unknown'),
            'execution_time_minutes': execution_data.get('execution_time_seconds', 0) / 60,
            'success': execution_data.get('success', False),
            'records_processed': execution_data.get('records_processed', 0),
            'data_quality_score': execution_data.get('data_quality_score', 1.0),
            'errors': execution_data.get('errors', []),
            'warnings': execution_data.get('warnings', [])
        }
        
        self.health_metrics['pipeline_runs'].append(execution_record)
        
        # Check SLA compliance
        self._check_sla_compliance(execution_record)
        
        # Check for performance degradation
        self._check_performance_trends()
        
        # Check error rate trends
        self._check_error_trends()
        
        logger.info("Pipeline execution recorded",
                   pipeline_name=execution_record['pipeline_name'],
                   success=execution_record['success'],
                   execution_time_minutes=execution_record['execution_time_minutes'],
                   records_processed=execution_record['records_processed'])
    
    def _check_sla_compliance(self, execution_record: Dict[str, Any]):
        """Check if execution meets SLA requirements"""
        
        violations = []
        
        # Check execution time SLA
        if execution_record['execution_time_minutes'] > self.sla_thresholds['max_execution_time_minutes']:
            violations.append({
                'type': 'execution_time',
                'threshold': self.sla_thresholds['max_execution_time_minutes'],
                'actual': execution_record['execution_time_minutes'],
                'severity': 'warning'
            })
        
        # Check data quality SLA
        if execution_record['data_quality_score'] < self.sla_thresholds['min_data_quality_score']:
            violations.append({
                'type': 'data_quality',
                'threshold': self.sla_thresholds['min_data_quality_score'],
                'actual': execution_record['data_quality_score'],
                'severity': 'error'
            })
        
        # Record SLA compliance
        sla_record = {
            'timestamp': execution_record['timestamp'],
            'pipeline_name': execution_record['pipeline_name'],
            'compliant': len(violations) == 0,
            'violations': violations
        }
        
        self.health_metrics['sla_compliance'].append(sla_record)
        
        # Send alerts for violations
        for violation in violations:
            self.alert_manager.send_alert(
                alert_type='sla_violation',
                title=f"SLA Violation: {violation['type']}",
                message=f"Pipeline {execution_record['pipeline_name']} violated {violation['type']} SLA. "
                       f"Threshold: {violation['threshold']}, Actual: {violation['actual']}",
                severity=violation['severity'],
                context={
                    'pipeline_name': execution_record['pipeline_name'],
                    'violation_type': violation['type'],
                    'threshold': violation['threshold'],
                    'actual_value': violation['actual']
                }
            )
    
    def _check_performance_trends(self):
        """Check for performance degradation trends"""
        
        recent_runs = self.health_metrics['pipeline_runs'][-10:]  # Last 10 runs
        if len(recent_runs) < 5:
            return  # Need at least 5 runs for trend analysis
        
        recent_times = [run['execution_time_minutes'] for run in recent_runs[-5:]]
        historical_times = [run['execution_time_minutes'] for run in recent_runs[-10:-5]]
        
        if not historical_times:
            return
        
        recent_avg = sum(recent_times) / len(recent_times)
        historical_avg = sum(historical_times) / len(historical_times)
        
        # Alert if recent performance is 50% worse than historical
        if recent_avg > historical_avg * 1.5:
            self.alert_manager.send_alert(
                alert_type='performance_degradation',
                title='Performance Degradation Detected',
                message=f'Recent pipeline execution time ({recent_avg:.2f} min) is significantly '
                       f'higher than historical average ({historical_avg:.2f} min)',
                severity='warning',
                context={
                    'recent_avg_minutes': recent_avg,
                    'historical_avg_minutes': historical_avg,
                    'degradation_percentage': ((recent_avg - historical_avg) / historical_avg) * 100
                }
            )
    
    def _check_error_trends(self):
        """Check for increasing error rates"""
        
        recent_runs = self.health_metrics['pipeline_runs'][-20:]  # Last 20 runs
        if len(recent_runs) < 10:
            return
        
        recent_error_rate = sum(1 for run in recent_runs[-10:] if not run['success']) / 10
        historical_error_rate = sum(1 for run in recent_runs[-20:-10] if not run['success']) / 10
        
        # Alert if error rate increased significantly
        if recent_error_rate > self.sla_thresholds['max_error_rate_percent'] / 100:
            self.alert_manager.send_alert(
                alert_type='high_error_rate',
                title='High Error Rate Detected',
                message=f'Pipeline error rate ({recent_error_rate*100:.1f}%) exceeds SLA threshold '
                       f'({self.sla_thresholds["max_error_rate_percent"]}%)',
                severity='error',
                context={
                    'current_error_rate_percent': recent_error_rate * 100,
                    'sla_threshold_percent': self.sla_thresholds['max_error_rate_percent'],
                    'recent_failures': [run for run in recent_runs[-10:] if not run['success']]
                }
            )
    
    def generate_health_dashboard(self) -> Dict[str, Any]:
        """Generate comprehensive health dashboard data"""
        
        recent_runs = self.health_metrics['pipeline_runs'][-50:]  # Last 50 runs
        
        if not recent_runs:
            return {'status': 'no_data', 'message': 'No pipeline execution data available'}
        
        # Calculate key metrics
        success_rate = sum(1 for run in recent_runs if run['success']) / len(recent_runs)
        avg_execution_time = sum(run['execution_time_minutes'] for run in recent_runs) / len(recent_runs)
        avg_data_quality = sum(run['data_quality_score'] for run in recent_runs) / len(recent_runs)
        total_records_processed = sum(run['records_processed'] for run in recent_runs)
        
        # Recent SLA compliance
        recent_sla = self.health_metrics['sla_compliance'][-10:]
        sla_compliance_rate = sum(1 for sla in recent_sla if sla['compliant']) / len(recent_sla) if recent_sla else 1.0
        
        # Overall health status
        health_status = 'healthy'
        if success_rate < self.sla_thresholds['min_success_rate_percent'] / 100:
            health_status = 'unhealthy'
        elif sla_compliance_rate < 0.8:  # Less than 80% SLA compliance
            health_status = 'degraded'
        elif avg_data_quality < self.sla_thresholds['min_data_quality_score']:
            health_status = 'degraded'
        
        dashboard = {
            'overall_status': health_status,
            'timestamp': datetime.utcnow().isoformat(),
            'summary': {
                'total_runs_analyzed': len(recent_runs),
                'success_rate_percent': round(success_rate * 100, 2),
                'avg_execution_time_minutes': round(avg_execution_time, 2),
                'avg_data_quality_score': round(avg_data_quality, 3),
                'sla_compliance_rate_percent': round(sla_compliance_rate * 100, 2),
                'total_records_processed': total_records_processed
            },
            'trends': {
                'recent_execution_times': [run['execution_time_minutes'] for run in recent_runs[-10:]],
                'recent_success_indicators': [run['success'] for run in recent_runs[-10:]],
                'recent_data_quality_scores': [run['data_quality_score'] for run in recent_runs[-10:]]
            },
            'sla_thresholds': self.sla_thresholds,
            'recent_violations': [
                sla for sla in self.health_metrics['sla_compliance'][-10:] 
                if not sla['compliant']
            ]
        }
        
        return dashboard

# Usage example
health_monitor = PipelineHealthMonitor(alert_manager)
```

This comprehensive error handling and monitoring framework provides enterprise-grade capabilities for production data pipelines. The patterns shown here enable robust, observable, and maintainable data engineering systems that can handle failures gracefully while providing actionable insights for operations teams.

Key features include structured logging, circuit breaker patterns, comprehensive data quality monitoring, intelligent alerting, and health dashboards that give full visibility into pipeline performance and reliability.
