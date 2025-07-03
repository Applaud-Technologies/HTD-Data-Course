# Universal ML Concepts for Data Engineers

## 1. Feature Engineering Patterns

### Time-based Features
**Recency**: How recently did an event occur?
- `days_since_last_purchase`, `hours_since_last_login`
- **Why it matters**: Recent behavior is often more predictive than old behavior

**Frequency**: How often does something happen?
- `purchases_per_month`, `login_frequency`, `support_tickets_per_quarter`
- **Why it matters**: Patterns of behavior reveal customer segments

**Trends**: Is something increasing or decreasing?
- `revenue_growth_rate`, `engagement_declining_flag`, `seasonal_patterns`
- **Why it matters**: Direction of change predicts future behavior

### Categorical Encoding Techniques
**One-hot Encoding**: Create binary columns for each category
- `color_red`, `color_blue`, `color_green` (0 or 1)
- **Use when**: Few categories (<10), no natural ordering

**Label Encoding**: Assign numbers to categories
- `size_small=1`, `size_medium=2`, `size_large=3`
- **Use when**: Categories have natural ordering

**Target Encoding**: Replace category with average target value
- `city_avg_churn_rate` instead of `city_name`
- **Use when**: Many categories, strong relationship to target

### Handling Missing Data in ML vs. Analytics
**Analytics**: Often filter out or ignore missing values
**ML**: Missing values can break models, requiring explicit handling

**Strategies**:
- **Fill with defaults**: Mean, median, mode, or business-specific values
- **Create indicator columns**: `is_missing_age` flag
- **Use algorithms that handle missing**: XGBoost, Random Forest
- **Impute intelligently**: Use related features to predict missing values

### Feature Selection and Dimensionality Reduction
**Why it matters**: Too many features can hurt model performance ("curse of dimensionality")

**Techniques**:
- **Statistical selection**: Keep features with strong correlation to target
- **Recursive elimination**: Remove least important features iteratively
- **Principal Component Analysis (PCA)**: Combine features into fewer dimensions
- **Domain knowledge**: Keep features that make business sense

### Feature Stores and Reusability
**Problem**: Teams rebuild the same features repeatedly
**Solution**: Centralized feature store with versioned, reusable features

**Benefits**:
- Consistency across projects
- Faster model development
- Automatic feature updates
- Point-in-time correctness

---

## 2. Model Evaluation Beyond Accuracy

### Business Metrics vs. Technical Metrics
**Technical Metrics**: Accuracy, precision, recall, F1-score
**Business Metrics**: Revenue impact, cost savings, customer satisfaction

**Example**:
- Model A: 95% accuracy, saves $100K/year
- Model B: 92% accuracy, saves $500K/year
- **Choose Model B** - business impact matters more than technical perfection

### A/B Testing for Model Performance
**Why**: Models can look good in testing but fail in production
**How**: Deploy competing models to different user segments
**Measure**: Business outcomes, not just technical metrics

**Example**:
- 50% of users see recommendations from Model A
- 50% see recommendations from Model B
- Compare click-through rates, conversion rates, revenue

### Fairness and Bias Detection
**Why it matters**: Models can perpetuate or amplify existing biases
**Common issues**:
- Hiring models that discriminate against protected groups
- Credit scoring that unfairly penalizes certain demographics
- Healthcare models that work poorly for underrepresented populations

**Detection methods**:
- Compare performance across demographic groups
- Test for disparate impact
- Use fairness-aware algorithms

### Explainability Requirements
**When needed**:
- Regulatory compliance (finance, healthcare)
- High-stakes decisions (hiring, lending)
- Building trust with stakeholders

**Techniques**:
- **SHAP**: Shows feature importance for each prediction
- **LIME**: Explains individual predictions
- **Feature importance**: Global view of what drives the model

### Performance Monitoring in Production
**What to monitor**:
- **Prediction accuracy**: Are we still making good predictions?
- **Data drift**: Is incoming data different from training data?
- **Model performance**: Response time, error rates, resource usage
- **Business metrics**: Is the model still delivering value?

**Alerting thresholds**:
- Accuracy drops below baseline
- Prediction distributions change significantly
- Business metrics decline

---

## 3. Data Quality for ML

### Training/Serving Skew
**Problem**: Model trained on one type of data but serves predictions on different data
**Examples**:
- Training on historical data, serving on real-time data
- Training on complete records, serving on partial records
- Training on batch data, serving on streaming data

**Solution**: Ensure training and serving data have identical preprocessing

### Data Drift Detection
**Concept drift**: The relationship between features and target changes
**Data drift**: The distribution of input features changes

**Detection methods**:
- Compare feature distributions over time
- Monitor model performance metrics
- Statistical tests for distribution changes

**Example**: COVID-19 changed shopping patterns, making pre-2020 models less accurate

### Label Quality and Consistency
**Why it matters**: Models learn from labels - bad labels create bad models
**Common issues**:
- Inconsistent labeling across annotators
- Mislabeled examples in training data
- Label drift over time

**Solutions**:
- Multi-annotator labeling with disagreement resolution
- Regular label quality audits
- Active learning to focus on uncertain examples

### Sampling Bias and Representativeness
**Problem**: Training data doesn't represent the real-world population
**Examples**:
- Survey data from only smartphone users
- Medical data from only one hospital
- Historical data that excludes recent trends

**Solutions**:
- Stratified sampling to ensure representation
- Regular data collection audits
- Bias testing on different population segments

### Data Lineage for Reproducibility
**Why it matters**: Need to recreate exact training conditions
**What to track**:
- Data sources and versions
- Preprocessing steps and parameters
- Feature engineering code versions
- Model training parameters

**Tools**: Data catalogs, version control, experiment tracking platforms

---

## 4. Common ML Failure Modes

### Distribution Shifts
**Problem**: Real-world data looks different from training data
**Examples**:
- Fraud detection model trained on summer data, deployed in winter
- Recommendation system trained on desktop users, deployed to mobile users
- Economic model trained during stable times, deployed during recession

**Detection**: Monitor input feature distributions
**Solution**: Retrain with recent data, use domain adaptation techniques

### Concept Drift
**Problem**: The relationship between inputs and outputs changes over time
**Examples**:
- Customer preferences evolve
- Market conditions change
- New competitors enter the market

**Detection**: Monitor model performance over time
**Solution**: Continuous retraining, online learning algorithms

### Data Leakage
**Problem**: Training data includes information that won't be available at prediction time
**Examples**:
- Using future information to predict past events
- Including the target variable in a transformed form
- Using data that's only available after the decision is made

**Prevention**: Careful temporal splitting, feature engineering review

### Overfitting to Training Data
**Problem**: Model memorizes training data instead of learning generalizable patterns
**Symptoms**: High training accuracy, poor test accuracy
**Solutions**:
- Cross-validation during training
- Regularization techniques
- More training data
- Simpler models

### Infrastructure Failures
**Common issues**:
- Model serving endpoints go down
- Database connections fail
- Memory or CPU limits exceeded
- Network latency increases

**Prevention**:
- Robust error handling
- Fallback mechanisms
- Resource monitoring
- Load testing

---

## Quick Reference: When to Be Concerned

### ⚠️ Red Flags
- **Accuracy gap**: Training accuracy >> test accuracy
- **Performance degradation**: Model works well initially, then performance drops
- **Inconsistent results**: Same input gives different outputs
- **Biased outcomes**: Model performs differently for different groups
- **Data anomalies**: Sudden spikes or drops in feature values

### ✅ Good Practices
- **Version everything**: Data, code, models, configurations
- **Monitor continuously**: Technical and business metrics
- **Test thoroughly**: Multiple datasets, edge cases, different populations
- **Document decisions**: Why this approach, what was tried, what didn't work
- **Plan for failure**: Fallback strategies, rollback procedures

### 📊 Essential Metrics to Track
- **Technical**: Accuracy, precision, recall, latency
- **Business**: Revenue impact, cost savings, user satisfaction
- **Data quality**: Completeness, consistency, freshness
- **Operational**: Uptime, error rates, resource usage

---

## Key Takeaways for Data Engineers

1. **Feature engineering is often more important than algorithm choice** - Focus on creating meaningful, predictive features

2. **Business impact matters more than technical perfection** - A simple model that drives business value beats a complex model that doesn't

3. **Data quality is crucial** - Clean, representative, unbiased data is the foundation of successful ML

4. **Plan for failure** - Models will break, data will drift, systems will fail - build resilience from the start

5. **Monitor everything** - What gets measured gets managed - track both technical and business metrics

6. **Collaboration is key** - Work closely with data scientists to understand requirements and constraints