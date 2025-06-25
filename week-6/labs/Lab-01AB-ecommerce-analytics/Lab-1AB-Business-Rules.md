# Business Rules for Lab01: E-Commerce Analytics


## Overview
This document defines the business rules students must implement to complete the TODO items in the e-commerce analytics lab.

---

## 1. Customer Segmentation Rules
**Function:** `create_enriched_orders_pipeline()`  
**Column:** `customer_segment`

### Rules:
- **High Value:** Customer tier is "Premium"
- **Medium Value:** Order value > $500
- **Standard:** All other customers

### Implementation Location:
```python
.withColumn(
    "customer_segment",
    # TODO: Apply customer segmentation rules above
)
```

---

## 2. Revenue Categorization Rules
**Function:** `create_enriched_orders_pipeline()`  
**Column:** `revenue_category`

### Rules:
- **High Revenue:** Order value > $1,000
- **Medium Revenue:** Order value > $200 
- **Low Revenue:** All other orders

### Implementation Location:
```python
.withColumn(
    "revenue_category", 
    # TODO: Apply revenue categorization rules above
)
```

---

## 3. Product Performance Classification
**Function:** `create_enriched_orders_pipeline()`  
**Column:** `product_performance`

### Rules:
- **Tech:** Product category is "Electronics"
- **Lifestyle:** Product category is "Books" OR "Sports"
- **General:** All other product categories

### Implementation Location:
```python
.withColumn(
    "product_performance",
    # TODO: Apply product performance classification rules above
)
```

---

## 4. Customer Analytics Business Rules
**Function:** `advanced_customer_analytics()`

### Customer Spending Categories:
- **HIGHEST_EVER:** Customer's spending percentile > 95%
- **VERY_HIGH:** Customer's spending percentile > 80%
- **VERY_LOW:** Customer's spending percentile < 20%
- **NORMAL:** All other spending levels

### Customer Ranking:
- Rank customers within their tier by total spending (highest first)
- Focus analysis on top 100 customers per tier

---

## 5. Inventory Optimization Rules
**Function:** `inventory_optimization_analysis()`

### Stock Level Recommendations:
- **Increase Stock:** 3-month rolling average > 1.5× current monthly demand
- **Reduce Stock:** 3-month rolling average < 0.5× current monthly demand  
- **Monitor Closely:** Demand volatility > 0.5× rolling average
- **Maintain Current Level:** All other scenarios

### Risk Assessment:
- **High Risk:** 
  - Increasing demand trend AND volatility > 10 units, OR
  - Decreasing demand trend AND 3-month average < 5 units
- **Medium Risk:** Demand volatility > rolling average
- **Low Risk:** All other scenarios

### Safety Stock Calculation:
- Recommended stock = (3-month rolling average × 1.2) + (demand volatility × 0.5)

---

## 6. Performance Measurement Rules
**Function:** `measure_performance_basic_vs_optimized()`

### Success Criteria:
- **Target:** 30%+ performance improvement
- **Good:** 10-29% performance improvement  
- **Needs Work:** <10% performance improvement

### Measurement Focus:
- Compare basic SQL joins vs. optimized broadcast joins
- Measure query execution time
- Count processed records for validation

---

## 7. Data Quality Validation Rules

### Completion Rate Calculation:
- Only count orders with status = "Completed"
- Calculate as: (Completed Orders ÷ Total Orders) × 100

### Data Validation Checks:
- No empty datasets after joins
- Customer IDs exist in both customers and orders tables
- Product IDs exist in both products and order_items tables
- Order IDs exist in both orders and order_items tables

---

## Usage Instructions for Students

1. **Reference these rules** when completing TODO items
2. **Use Spark's `when().otherwise()` syntax** for conditional logic
3. **Apply `.cache()` method** after adding business logic calculations
4. **Validate results** using the data quality rules above
5. **Test performance** using the measurement criteria provided

These business rules reflect real-world e-commerce analytics requirements and demonstrate proper data engineering practices.