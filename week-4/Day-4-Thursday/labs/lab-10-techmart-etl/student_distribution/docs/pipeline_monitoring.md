# TechMart ETL Pipeline - Monitoring and Metrics Guide

## Overview

This document provides comprehensive guidance for monitoring the TechMart ETL pipeline, understanding generated metrics, and integrating with Power BI dashboards. The monitoring system captures operational performance, data quality indicators, and business metrics to ensure pipeline reliability and stakeholder visibility.

## Monitoring Architecture

### Three-Layer Monitoring Approach

1. **Operational Monitoring**: Pipeline performance, system health, and technical metrics
2. **Data Quality Monitoring**: Data validation results, quality scores, and issue tracking
3. **Business Monitoring**: Revenue metrics, customer analytics, and business KPIs

### Data Flow
```
ETL Pipeline Execution
       ↓
Metrics Collection (monitoring.py)
       ↓
ETL Metadata Tables (etl_pipeline_runs, data_quality_issues)
       ↓
Power BI Dashboard (Operational & Business Views)
```

---

## Operational Metrics

### Pipeline Execution Metrics

#### Core Performance Indicators
| Metric Name | Description | Target/SLA | Business Impact |
|-------------|-------------|------------|-----------------|
| `total_duration_seconds` | Complete pipeline runtime | < 300 seconds (5 min) | Data freshness for business reporting |
| `extraction_duration_seconds` | Data extraction phase time | < 60 seconds (1 min) | Source system load impact |
| `transformation_duration_seconds` | Data transformation time | < 120 seconds (2 min) | Processing efficiency |
| `loading_duration_seconds` | Data warehouse loading time | < 60 seconds (1 min) | Analytics availability |
| `records_per_minute` | Processing throughput | > 200 records/min | Scalability indicator |
| `pipeline_success_rate` | Success percentage (24h) | > 95% | Reliability measure |

#### Resource Utilization
| Metric Name | Description | Target/SLA | Monitoring Purpose |
|-------------|-------------|------------|-------------------|
| `peak_memory_mb` | Maximum memory usage | < 1024 MB (1 GB) | Resource planning |
| `cpu_utilization_percent` | Peak CPU usage | < 80% | Performance optimization |
| `disk_io_operations` | Disk I/O count | Baseline tracking | Storage performance |
| `network_bytes_transferred` | Network data transfer | Baseline tracking | Network impact |

#### Source System Health
| Metric Name | Description | Target/SLA | Business Impact |
|-------------|-------------|------------|-----------------|
| `csv_extraction_success` | CSV file read success | 100% | Sales data availability |
| `json_extraction_success` | JSON file parse success | 100% | Product catalog updates |
| `mongodb_connection_success` | MongoDB connectivity | 99% | Customer profile updates |
| `sqlserver_connection_success` | SQL Server connectivity | 99% | Support data integration |
| `data_freshness_hours` | Data age at processing | < 24 hours | Business reporting timeliness |

### Error and Alert Metrics

#### Error Categories
| Error Type | Description | Severity | Action Required |
|------------|-------------|----------|-----------------|
| `CONNECTION_ERROR` | Database connectivity issues | High | Check network/credentials |
| `FILE_NOT_FOUND` | Missing source files | High | Verify file delivery |
| `SCHEMA_MISMATCH` | Unexpected data structure | Medium | Update transformation logic |
| `DATA_QUALITY_FAILURE` | Quality below threshold | Medium | Review data sources |
| `LOADING_ERROR` | Data warehouse insert failure | High | Check DW connectivity |

#### Alert Thresholds
| Metric | Warning Threshold | Critical Threshold | Notification |
|--------|-------------------|-------------------|--------------|
| Pipeline Duration | > 240 seconds | > 360 seconds | Email team |
| Data Quality Score | < 0.85 | < 0.7 | Slack alert |
| Error Rate | > 5% | > 15% | Page on-call |
| Memory Usage | > 800 MB | > 1200 MB | Email admin |

---

## Data Quality Metrics

### Quality Scoring Framework

#### Overall Data Quality Score
```
Data Quality Score = (Completeness × 0.4) + (Accuracy × 0.3) + (Consistency × 0.2) + (Timeliness × 0.1)
```

#### Component Scores
| Quality Dimension | Description | Calculation | Target |
|------------------|-------------|-------------|--------|
| **Completeness** | Percentage of non-null required fields | (Non-null values / Total values) × 100 | > 90% |
| **Accuracy** | Percentage of valid format/range values | (Valid values / Total values) × 100 | > 95% |
| **Consistency** | Cross-source data consistency | (Consistent records / Total records) × 100 | > 90% |
| **Timeliness** | Data freshness and currency | Age-based scoring algorithm | > 85% |

### Source-Specific Quality Metrics

#### CSV Sales Data Quality
| Metric | Description | Calculation | Target |
|--------|-------------|-------------|--------|
| `csv_completeness_score` | Required field completeness | Non-null customer_id, product_id, quantity, price | > 95% |
| `csv_validity_score` | Value validity percentage | Valid dates, positive quantities/prices | > 98% |
| `csv_duplicate_rate` | Duplicate transaction percentage | Duplicate transaction_ids / Total transactions | < 1% |
| `csv_orphan_rate` | Invalid reference percentage | Invalid customer/product IDs / Total records | < 2% |

#### JSON Product Quality
| Metric | Description | Calculation | Target |
|--------|-------------|-------------|--------|
| `json_schema_compliance` | Schema adherence percentage | Records with required fields / Total records | > 90% |
| `json_price_validity` | Valid pricing percentage | Records with valid prices / Total records | > 99% |
| `json_category_consistency` | Category standardization | Standard categories / Total categories | > 95% |
| `json_inventory_accuracy` | Inventory data accuracy | Valid stock levels / Total products | > 90% |

#### MongoDB Customer Quality
| Metric | Description | Calculation | Target |
|--------|-------------|-------------|--------|
| `mongodb_profile_completeness` | Profile completeness | Complete profiles / Total profiles | > 80% |
| `mongodb_email_validity` | Valid email percentage | Valid email formats / Total emails | > 95% |
| `mongodb_schema_consistency` | Field naming consistency | Consistent field names / Total variations | > 85% |
| `mongodb_duplicate_customers` | Customer deduplication | Unique customers / Total customer records | > 99% |

#### SQL Server Support Quality
| Metric | Description | Calculation | Target |
|--------|-------------|-------------|--------|
| `sqlserver_referential_integrity` | Valid customer references | Valid customer_ids / Total tickets | > 98% |
| `sqlserver_date_consistency` | Date logic validation | Valid date sequences / Total tickets | > 99% |
| `sqlserver_status_validity` | Valid status values | Valid statuses / Total tickets | > 99% |
| `sqlserver_satisfaction_range` | Rating range validation | Valid ratings (1-5) / Total ratings | > 95% |

### Quality Issue Tracking

#### Issue Classification
| Issue Type | Description | Severity | Typical Resolution |
|------------|-------------|----------|-------------------|
| `MISSING_VALUE` | Required field is null/empty | High | Source system fix |
| `INVALID_FORMAT` | Format validation failure | Medium | Data transformation |
| `RANGE_VIOLATION` | Value outside expected range | Medium | Business rule review |
| `DUPLICATE_RECORD` | Duplicate business key | High | Deduplication logic |
| `ORPHANED_RECORD` | Missing foreign key reference | High | Reference data fix |
| `SCHEMA_DRIFT` | Unexpected field structure | Low | Schema update |

#### Quality Trend Analysis
| Trend Metric | Description | Calculation | Action Threshold |
|-------------|-------------|-------------|------------------|
| `quality_trend_7d` | 7-day quality trend | Current score vs 7-day average | ±5% change |
| `quality_volatility` | Quality score stability | Standard deviation of daily scores | > 0.1 volatility |
| `issue_recurrence_rate` | Recurring issue percentage | Repeat issues / Total issues | > 20% recurrence |
| `resolution_time_avg` | Average issue resolution time | Hours from detection to resolution | > 24 hours |

---

## Business Metrics for Dashboard

### Revenue and Sales Metrics

#### Core Business KPIs
| Metric Name | Description | Calculation | Business Purpose |
|-------------|-------------|-------------|------------------|
| `total_revenue_daily` | Daily revenue total | SUM(fact_sales.total_amount) | Daily performance tracking |
| `total_transactions_daily` | Daily transaction count | COUNT(fact_sales) | Volume monitoring |
| `average_order_value` | Average transaction value | total_revenue / total_transactions | Customer spending behavior |
| `revenue_growth_rate` | Revenue growth percentage | (Current - Previous) / Previous × 100 | Business growth tracking |
| `transactions_per_customer` | Customer transaction frequency | transactions / unique_customers | Customer engagement |

#### Product Performance
| Metric Name | Description | Calculation | Business Purpose |
|-------------|-------------|-------------|------------------|
| `top_selling_categories` | Best performing categories | Revenue by category (top 5) | Inventory planning |
| `product_profit_margins` | Product profitability | AVG(margin_percent) by product | Pricing optimization |
| `inventory_turnover` | Stock movement rate | Sales volume / average inventory | Inventory management |
| `new_product_performance` | New product sales rate | Sales of products < 90 days old | Product launch tracking |
| `product_return_rate` | Return percentage | Returns / Total sales by product | Quality monitoring |

### Customer Analytics

#### Customer Segmentation
| Metric Name | Description | Calculation | Business Purpose |
|-------------|-------------|-------------|------------------|
| `customer_acquisition_rate` | New customer percentage | New customers / Total customers | Growth tracking |
| `customer_retention_rate` | Returning customer percentage | Repeat customers / Total customers | Loyalty measurement |
| `loyalty_tier_distribution` | Customer tier breakdown | Count by Bronze/Silver/Gold/Platinum | Loyalty program effectiveness |
| `customer_lifetime_value` | Average customer value | AVG(total_lifetime_value) | Customer value assessment |
| `high_value_customer_count` | Premium customer count | Customers with LTV > threshold | VIP customer tracking |

#### Customer Satisfaction
| Metric Name | Description | Calculation | Business Purpose |
|-------------|-------------|-------------|------------------|
| `overall_satisfaction_score` | Average satisfaction rating | AVG(customer_satisfaction_score) | Service quality indicator |
| `support_ticket_volume` | Daily support tickets | COUNT(fact_customer_support) | Support workload |
| `first_response_time_avg` | Average first response time | AVG(first_response_time_hours) | Support efficiency |
| `resolution_rate` | Ticket resolution percentage | Resolved tickets / Total tickets | Support effectiveness |
| `customer_effort_score` | Average resolution time | AVG(resolution_time_hours) | Customer experience |

### Operational Business Metrics

#### Channel Performance
| Metric Name | Description | Calculation | Business Purpose |
|-------------|-------------|-------------|------------------|
| `online_sales_percentage` | Online channel share | Online sales / Total sales | Channel strategy |
| `mobile_conversion_rate` | Mobile sales effectiveness | Mobile sales / Mobile visits | Mobile optimization |
| `store_performance_ranking` | Store ranking by revenue | Revenue by store (ranked) | Store management |
| `channel_profitability` | Profit by sales channel | Profit by channel / Channel sales | Channel ROI |

#### Geographic Performance
| Metric Name | Description | Calculation | Business Purpose |
|-------------|-------------|-------------|------------------|
| `sales_by_region` | Regional sales breakdown | Sales by state/region | Market analysis |
| `regional_growth_rates` | Growth by geography | Regional growth percentages | Expansion planning |
| `shipping_cost_analysis` | Fulfillment cost by region | Shipping costs / Sales by region | Logistics optimization |

---

## Power BI Dashboard Integration

### Dashboard Structure

#### Executive Summary Dashboard
**Purpose**: High-level business performance for executives
**Refresh**: Every 4 hours
**Key Visuals**:
- Revenue trend (line chart)
- Customer acquisition (gauge)
- Product performance (bar chart)
- Geographic sales (map)
- Key metrics cards

#### Operational Dashboard
**Purpose**: ETL pipeline performance for IT operations
**Refresh**: Every 15 minutes
**Key Visuals**:
- Pipeline status (indicator)
- Performance trends (line chart)
- Error rate (gauge)
- Data quality score (gauge)
- System resource usage (bar chart)

#### Data Quality Dashboard
**Purpose**: Data quality monitoring for data stewards
**Refresh**: After each ETL run
**Key Visuals**:
- Quality score trends (line chart)
- Issues by source (stacked bar)
- Quality heat map (matrix)
- Issue resolution tracking (table)

### Data Source Configuration

#### ETL Metadata Connection
```json
{
  "dataSource": {
    "type": "SqlServer",
    "server": "your-dw-server",
    "database": "TechMart_Analytics_DW",
    "tables": [
      "etl_pipeline_runs",
      "data_quality_issues",
      "vw_business_summary",
      "vw_customer_analytics",
      "vw_product_performance"
    ]
  }
}
```

#### Key Measures (DAX)
```dax
// Pipeline Success Rate
Pipeline Success Rate = 
DIVIDE(
    COUNTROWS(FILTER(etl_pipeline_runs, etl_pipeline_runs[status] = "SUCCESS")),
    COUNTROWS(etl_pipeline_runs)
) * 100

// Data Quality Trend
Quality Trend = 
VAR CurrentScore = AVERAGE(etl_pipeline_runs[data_quality_score])
VAR PreviousScore = CALCULATE(
    AVERAGE(etl_pipeline_runs[data_quality_score]),
    DATEADD(DimDate[Date], -7, DAY)
)
RETURN CurrentScore - PreviousScore

// Revenue Growth Rate
Revenue Growth = 
VAR CurrentRevenue = SUM(fact_sales[total_amount])
VAR PreviousRevenue = CALCULATE(
    SUM(fact_sales[total_amount]),
    DATEADD(DimDate[Date], -1, MONTH)
)
RETURN DIVIDE(CurrentRevenue - PreviousRevenue, PreviousRevenue) * 100
```

### Dashboard Refresh Strategy

#### Automated Refresh Schedule
| Dashboard Type | Refresh Frequency | Trigger | Data Latency |
|---------------|-------------------|---------|--------------|
| Executive Summary | Every 4 hours | Scheduled | 4-8 hours |
| Operational | Every 15 minutes | Scheduled | 15-30 minutes |
| Data Quality | ETL completion | Event-driven | 5-10 minutes |

#### Refresh Dependencies
```
ETL Pipeline Completion
       ↓
ETL Metadata Update
       ↓
Power BI Dataset Refresh
       ↓
Dashboard Update
       ↓
Email Notifications (if configured)
```

---

## Alerting and Notifications

### Alert Configuration

#### Critical Alerts (Immediate Response)
| Alert Condition | Notification Method | Recipients | Response SLA |
|----------------|-------------------|------------|--------------|
| Pipeline failure | Email + Slack | Data team, Manager | 15 minutes |
| Data quality < 0.7 | Email + SMS | Data steward, Analyst | 30 minutes |
| Processing time > 360s | Email | IT operations | 1 hour |
| Source connection failure | Slack | Infrastructure team | 30 minutes |

#### Warning Alerts (Monitoring)
| Alert Condition | Notification Method | Recipients | Response SLA |
|----------------|-------------------|------------|--------------|
| Data quality < 0.85 | Email | Data steward | 2 hours |
| Processing time > 240s | Slack | Data team | 4 hours |
| Memory usage > 800MB | Email | IT operations | Next business day |
| Error rate > 5% | Slack | Data team | 4 hours |

### Alert Templates

#### Pipeline Failure Alert
```
Subject: [CRITICAL] TechMart ETL Pipeline Failure - Run ID: {run_id}

Pipeline Status: FAILED
Run ID: {run_id}
Start Time: {start_time}
Failure Time: {failure_time}
Error: {error_message}

Impact: Business reporting data may be stale
Action Required: Investigate and resolve within 15 minutes

Dashboard: {operational_dashboard_link}
Logs: {log_file_link}
```

#### Data Quality Warning
```
Subject: [WARNING] Data Quality Below Threshold - Score: {quality_score}

Data Quality Alert
Score: {quality_score} (Target: > 0.85)
Run ID: {run_id}
Affected Sources: {affected_sources}

Top Issues:
{top_issues_list}

Impact: May affect business reporting accuracy
Action: Review data quality dashboard and address issues

Dashboard: {quality_dashboard_link}
```

---

## Troubleshooting Guide

### Common Issues and Resolutions

#### Pipeline Performance Issues
| Symptom | Likely Cause | Investigation Steps | Resolution |
|---------|--------------|-------------------|------------|
| Slow extraction | Large data volumes | Check source record counts | Implement incremental extraction |
| High memory usage | Large datasets in memory | Monitor batch sizes | Reduce batch size, optimize queries |
| Timeout errors | Network/database latency | Check connection logs | Increase timeout settings |
| Frequent failures | Resource contention | Check system resources | Schedule during off-peak hours |

#### Data Quality Issues
| Symptom | Likely Cause | Investigation Steps | Resolution |
|---------|--------------|-------------------|------------|
| Dropping quality scores | Source data degradation | Review quality issue log | Engage with source system owners |
| Missing data | Source system issues | Check extraction logs | Implement retry logic |
| Format validation failures | Schema changes | Compare current vs previous data | Update validation rules |
| Duplicate records | Source system duplicates | Analyze duplicate patterns | Implement deduplication logic |

#### Dashboard Issues
| Symptom | Likely Cause | Investigation Steps | Resolution |
|---------|--------------|-------------------|------------|
| Stale dashboard data | Refresh failures | Check Power BI service logs | Fix data source connections |
| Missing metrics | ETL metadata issues | Verify metadata table population | Review ETL metadata loading |
| Incorrect calculations | DAX measure errors | Test measures in Power BI Desktop | Update DAX formulas |
| Slow dashboard performance | Large datasets | Review query performance | Optimize data model |

### Monitoring Best Practices

#### Daily Monitoring Checklist
- [ ] Check pipeline execution status
- [ ] Review data quality scores
- [ ] Verify dashboard refresh success
- [ ] Monitor system resource usage
- [ ] Review error logs for patterns

#### Weekly Monitoring Tasks
- [ ] Analyze performance trends
- [ ] Review data quality trends
- [ ] Update alert thresholds if needed
- [ ] Check dashboard usage metrics
- [ ] Review and update documentation

#### Monthly Monitoring Review
- [ ] Evaluate SLA compliance
- [ ] Review and update monitoring strategy
- [ ] Analyze capacity planning needs
- [ ] Update business metrics as needed
- [ ] Stakeholder feedback review

---

## Performance Optimization

### Monitoring-Driven Optimization

#### Performance Metrics Analysis
1. **Trend Analysis**: Identify performance degradation over time
2. **Bottleneck Identification**: Find slowest pipeline components
3. **Resource Utilization**: Optimize CPU, memory, and I/O usage
4. **Capacity Planning**: Project future resource needs

#### Optimization Strategies
| Performance Issue | Optimization Approach | Expected Impact |
|------------------|----------------------|-----------------|
| Slow extraction | Parallel processing, incremental loads | 50-70% improvement |
| Memory pressure | Streaming processing, batch optimization | 30-50% reduction |
| Slow transformations | Vectorization, efficient algorithms | 40-60% improvement |
| Loading bottlenecks | Bulk inserts, index optimization | 60-80% improvement |

### Continuous Improvement Process

#### Monthly Performance Review
1. **Metric Collection**: Gather 30 days of performance data
2. **Trend Analysis**: Identify patterns and degradation
3. **Root Cause Analysis**: Investigate performance issues
4. **Optimization Planning**: Plan improvements for next month
5. **Implementation**: Execute optimizations
6. **Validation**: Measure improvement impact

This monitoring guide provides the foundation for maintaining a reliable, high-performance ETL pipeline with comprehensive visibility into operational performance, data quality, and business impact. Regular review and optimization of these monitoring practices ensures the pipeline continues to meet business needs as data volumes and requirements grow.