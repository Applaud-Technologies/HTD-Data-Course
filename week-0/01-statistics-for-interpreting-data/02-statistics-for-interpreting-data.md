# Statistics for Interpreting Data



## Introduction

Every alert threshold you configure, every performance dashboard you scan, and every incident you troubleshoot relies on statistical understanding—whether you realize it or not. 

As software systems grow more complex, the ability to correctly interpret data patterns becomes increasingly crucial. When a service shows degraded performance, statistical thinking helps you distinguish between random fluctuation and actual problems requiring intervention. Similarly, when optimizing code, statistical analysis reveals whether your changes made a measurable difference or merely coincided with normal variations. 

This lesson equips you with foundational statistical concepts tailored specifically for software engineering contexts, from analyzing system metrics to interpreting user behavior patterns.



## Learning Outcomes

By the end of this lesson, you will be able to:

1. Differentiate between continuous and discrete data types to select appropriate statistical analysis methods for software metrics.
2. Calculate and interpret measures of central tendency and variance to summarize dataset characteristics in system monitoring contexts.
3. Apply probability concepts to sample spaces to evaluate the statistical significance of findings in software testing and error analysis.
4. Identify meaningful trends, seasonality, and cyclical patterns in time-series data to support capacity planning and anomaly detection.
5. Detect and interpret outliers using statistical methods to distinguish between measurement errors, legitimate edge cases, and system anomalies.
6. Distinguish between correlation and causation when analyzing relationships between variables to avoid misattribution in performance analysis.



## Understanding Data Types: Continuous vs Discrete

### Characteristics of Continuous and Discrete Data

At the foundation of statistical analysis lies the distinction between continuous and discrete data. This distinction dictates how we should visualize, analyze, and interpret our findings.

**Discrete data** consists of finite, countable values that cannot be subdivided meaningfully. Common examples include:
- Number of HTTP requests to an API
- Count of exceptions thrown
- User click events
- Server restarts

**Continuous data** encompasses measurements that can take any value within a range, including fractional values. Examples include:
- Response time in milliseconds
- Memory usage percentage
- CPU temperature
- Download speeds

The distinction impacts how we process data in our systems. For discrete data, we typically count occurrences and work with whole numbers. For continuous data, we measure values along a spectrum, often requiring different statistical treatments.

When building data processing pipelines, this distinction affects everything from storage decisions to analysis approaches. Discrete data often benefits from integer-based optimizations and frequency-based analyses, while continuous data requires range-based operations and distributional analyses. Even your choice of database indexing strategy may differ based on whether you're primarily working with discrete or continuous values.



![Diagram: Discrete vs Continuous Data in Software Systems](D:\_TheVault\2_Work\AT-CRS-HTD\01-Data Engineering Course\_week-01\01 Data Literacy\02-statistics-for-interpreting-data\_assets\diagram-discrete-vs-continuous-data-in-software-systems.png)


> **Diagram: Discrete vs Continuous Data in Software Systems**
> 
> This diagram clarifies the fundamental difference between discrete and continuous data types, which is essential for choosing appropriate statistical methods. The hierarchical structure helps students mentally organize these concepts and provides relevant software engineering examples.



### Visualizing Different Data Types

Choosing the appropriate visualization is crucial for accurate interpretation. For discrete data, bar charts and frequency tables work well as they emphasize the distinct, countable nature of the values.

```python
# Visualizing discrete data: API errors by type
import matplotlib.pyplot as plt

error_types = ['404', '500', '403', '429', '502']
counts = [142, 37, 88, 103, 22]

plt.figure(figsize=(10, 6))
plt.bar(error_types, counts)
plt.title('API Errors by Type')
plt.xlabel('Error Code')
plt.ylabel('Count')
plt.show()
```



For continuous data, histograms and density plots better represent the distribution across a range of values:

```python
# Visualizing continuous data: API response times
import numpy as np
import matplotlib.pyplot as plt

# Generate sample response times (ms)
response_times = np.random.gamma(shape=2, scale=50, size=1000)

plt.figure(figsize=(10, 6))
plt.hist(response_times, bins=30, alpha=0.7)
plt.title('API Response Time Distribution')
plt.xlabel('Response Time (ms)')
plt.ylabel('Frequency')
plt.axvline(np.mean(response_times), color='r', linestyle='dashed', linewidth=1)
plt.show()
```

A distribution plot visually maps the frequency or density of values across the measurement range. For continuous data, this appears as a smooth curve, while discrete data produces distinct spikes at countable intervals. Understanding this visual difference helps identify the appropriate statistical measures to apply.

The bin selection in histograms is particularly important when working with software metrics. Too few bins might obscure important patterns in your performance data, while too many can create visual noise. For monitoring data, adaptive binning strategies that adjust based on data density often yield more insightful visualizations than fixed-width approaches. This becomes especially important when comparing metrics across different timeframes or system components.



## Measures of Central Tendency and Variance

### Calculating Mean, Median, and Mode

Central tendency measures help summarize datasets with single representative values, providing different perspectives on what constitutes the "middle" or "typical" value.

**Mean**: The arithmetic average, calculated by summing all values and dividing by the count.
```python
def calculate_mean(values):
    return sum(values) / len(values)
    
# Example: Average response times
response_times = [215, 187, 302, 198, 241, 179, 225]
mean_time = calculate_mean(response_times)  # 221 ms
```



**Median**: The middle value when all observations are arranged in order. For even-sized sets, it's the average of the two middle values.

```python
def calculate_median(values):
    sorted_values = sorted(values)
    n = len(sorted_values)
    mid = n // 2
    
    if n % 2 == 0:
        return (sorted_values[mid-1] + sorted_values[mid]) / 2
    else:
        return sorted_values[mid]
        
# Median of the same response times
median_time = calculate_median(response_times)  # 215 ms
```



**Mode**: The most frequently occurring value(s).

```python
def calculate_mode(values):
    frequency = {}
    for value in values:
        frequency[value] = frequency.get(value, 0) + 1
    
    max_freq = max(frequency.values())
    return [k for k, v in frequency.items() if v == max_freq]
```





![Diagram: Central Tendency Measure Usage in Software Metrics](D:\_TheVault\2_Work\AT-CRS-HTD\01-Data Engineering Course\_week-01\01 Data Literacy\02-statistics-for-interpreting-data\_assets\diagram-central-tendency-measure-usage-in-software-metrics.png)

> **Diagram: Central Tendency Measure Usage in Software Metrics**
> 
> This pie chart illustrates which central tendency measures are most appropriate for different software metrics. It provides a quick visual reference showing that response times typically use median, error rates use mean, etc.





For performance metrics, the choice of central tendency measure matters significantly:

- **Mean** is useful for normally distributed data but is sensitive to outliers
- **Median** is better for skewed distributions like response times (resistant to outliers)
- **Mode** helps identify common discrete values (e.g., most frequent error codes)

In telemetry systems for production applications, the median often provides more reliable insights than the mean for metrics like response times or resource utilization. A single slow database query or memory spike can drastically inflate the mean without representing the typical user experience. Many monitoring systems now default to reporting percentiles (like p50/median, p90, p99) rather than means for this reason.

.

![Diagram: Statistical Measure Selection Flowchart](D:\_TheVault\2_Work\AT-CRS-HTD\01-Data Engineering Course\_week-01\01 Data Literacy\02-statistics-for-interpreting-data\_assets\diagram-statistical-measure-selection-flowchart.png)




> **Diagram: Statistical Measure Selection Flowchart**
> 
> This decision tree guides students through selecting appropriate statistical measures based on data distribution characteristics. It shows the logical process of determining which measures to use when analyzing software metrics.



### 

### Understanding Variance and Standard Deviation

While central tendency describes the "middle," variance measures tell us about data spread or dispersion.

**Variance** quantifies the average squared deviation from the mean:
```python
def calculate_variance(values):
    mean = calculate_mean(values)
    return sum((x - mean) ** 2 for x in values) / len(values)
```



**Standard Deviation** is the square root of variance, expressing spread in the same units as the original data:

```python
def calculate_std_dev(values):
    return calculate_variance(values) ** 0.5
```



For a normal distribution, approximately:

- 68% of values fall within ±1 standard deviation of the mean
- 95% of values fall within ±2 standard deviations
- 99.7% of values fall within ±3 standard deviations

In system monitoring, standard deviation helps establish normal operating ranges. For example, if CPU usage has a mean of 45% with a standard deviation of 8%, values above 61% (mean + 2σ) might warrant investigation as potential outliers.

Z-scores standardize individual observations by expressing how many standard deviations they are from the mean:
```python
z_score = (value - mean) / std_dev
```

This normalization allows comparison across different metrics and establishes consistent thresholds for anomaly detection in monitoring systems.

When implementing alerting systems, z-scores provide a statistically sound approach to setting thresholds. Rather than using arbitrary cutoffs like "alert if CPU > 80%," you might trigger alerts when metrics exceed their historical mean by 2.5 standard deviations. This approach automatically adjusts to the natural variability of each system component, reducing false positives in noisy metrics and increasing sensitivity for typically stable ones.



## Sample Spaces and Probability Concepts

### Working with Sample Spaces

A **sample space** represents all possible outcomes of a process or experiment. In software engineering, we frequently work with sample spaces when modeling potential system states, test outcomes, or user behaviors.

Consider a simple availability check for a three-server system:

```
Sample Space = {
    (up, up, up),
    (up, up, down),
    (up, down, up),
    (up, down, down),
    (down, up, up),
    (down, up, down),
    (down, down, up),
    (down, down, down)
}
```

This sample space contains 2³ = 8 possible states. An **event** is a subset of the sample space that we're interested in analyzing. For example, "at least two servers are running" would include the first four elements.

When calculating test coverage, we implicitly work with sample spaces. If a function has three decision points with two possibilities each, complete coverage requires testing 2³ = 8 possible paths through the function.

The concept of sample spaces provides a rigorous framework for fault tolerance analysis. In distributed systems, combinatorial analysis is extensively used to ensure resilience against various failure modes. For instance, when designing a system to survive the failure of any k out of n components, you're essentially working with a sample space of (n choose k) possible failure combinations, and testing whether each of these events leaves the system in an acceptable state.



### Applying Probability in Statistical Analysis

Probability quantifies the likelihood of events occurring within a sample space. For discrete events, probability is calculated as:

```
P(Event) = Number of favorable outcomes / Total number of possible outcomes
```

For our server example, the probability of all servers being up might be:
```
P(all up) = 1/8 = 0.125
```

However, real systems have non-uniform probabilities. If each server has 99% uptime independently, then:
```
P(all up) = 0.99 × 0.99 × 0.99 ≈ 0.97
```



**Conditional probability** expresses how the occurrence of one event affects the probability of another. For two events A and B:

```
P(A|B) = P(A and B) / P(B)
```

This becomes crucial when analyzing error conditions. For example, if 5% of requests fail, but among mobile users, 12% of requests fail:

```
P(failure | mobile) = 0.12
```

This indicates mobile users experience a higher failure rate, warranting investigation.



**Bayes' theorem** allows us to reverse conditional probabilities:

```
P(A|B) = P(B|A) × P(A) / P(B)
```

In error analysis, we might know P(error|condition) but need P(condition|error) to diagnose issues effectively. For instance, if we observe an error spike, Bayes' theorem helps calculate the probability that specific conditions (like high traffic) caused the errors.

Bayesian analysis has become increasingly important in modern ops environments, particularly for incident response. If we detect a system error, we want to know which component most likely caused it. While traditional monitoring focuses on P(error|component) — "what's the chance of seeing an error if component X fails?" — what we really need during incidents is P(component|error) — "given that we're seeing this error, which component is most likely the culprit?" This Bayesian inversion helps engineers prioritize their investigation efforts during outages.

The diagram below illustrates conditional probability with overlapping sets of events in a Venn diagram, where the intersection represents outcomes where both events occur simultaneously.



## Analyzing Time-Series Data

### Identifying Trends and Patterns

Time-series data—measurements collected at regular intervals—form the backbone of system monitoring and performance analysis. Identifying meaningful trends in this data allows engineers to proactively address issues before they affect users.

A **trend** represents the underlying direction of a time series, typically categorized as:

- **Uptrend**: Values generally increase over time (e.g., growing memory usage)
- **Downtrend**: Values generally decrease over time (e.g., declining response times after optimization)
- **Sideways/Range**: No clear directional movement (e.g., stable CPU utilization)



Consider this pseudo-analysis of response time data:

```python
# Simple trend detection
def detect_trend(time_series, window_size=5):
    n = len(time_series)
    if n < window_size * 2:
        return "Insufficient data"
    
    # Compare average of first window to last window
    first_window = sum(time_series[:window_size]) / window_size
    last_window = sum(time_series[-window_size:]) / window_size
    
    diff = last_window - first_window
    if abs(diff) < first_window * 0.1:  # Less than 10% change
        return "Range"
    elif diff > 0:
        return "Uptrend"
    else:
        return "Downtrend"
```

When analyzing trends, it's essential to consider the timeframe—what appears as a trend in one timeframe might be part of a larger pattern in another.

Production trend analysis typically requires more sophisticated approaches than simple window comparisons. Techniques like exponential smoothing, ARIMA models, and moving average convergence-divergence (MACD) can help differentiate genuine trends from noise. Many monitoring systems now incorporate these methods to distinguish between normal fluctuations and actionable shifts in system behavior, reducing alert fatigue while maintaining sensitivity to meaningful changes.



### Measuring Seasonality and Cycles

Beyond directional trends, time-series data often exhibits **seasonal patterns** (regular, predictable fluctuations) and **cyclical behaviors** (irregular fluctuations).

Common patterns in software systems include:

- **Daily patterns**: Higher traffic during business hours
- **Weekly patterns**: Reduced weekend activity
- **Monthly patterns**: Billing-related spikes near month-end
- **Quarterly patterns**: Business reporting cycles
- **Annual patterns**: Holiday traffic or end-of-year activities



Identifying these patterns requires looking beyond simple trend analysis. Decomposition methods separate time series into:

1. **Trend component**: The long-term direction
2. **Seasonal component**: Repeating patterns of known frequency
3. **Cyclical component**: Irregular but recurring patterns
4. **Residual component**: Unexplained variation



A simple approach to detect seasonality compares current values to those from previous cycles:

```python
def has_daily_seasonality(hourly_data, days=7):
    # Simplified detection: Compare correlation between days
    hours_per_day = 24
    current_day = hourly_data[-hours_per_day:]
    
    for i in range(1, min(days, len(hourly_data) // hours_per_day)):
        previous_day = hourly_data[-(i+1)*hours_per_day:-i*hours_per_day]
        correlation = calculate_correlation(current_day, previous_day)
        if correlation > 0.7:  # Strong correlation threshold
            return True
    
    return False
```



For software systems, recognizing these patterns enables:

- Capacity planning for predictable demand
- Scheduled maintenance during low-activity periods
- Anomaly detection by comparing against expected patterns
- Distinguishing between normal seasonal variation and actual issues

Understanding seasonality is particularly important for service-level objective (SLO) setting and capacity planning. A system that handles 10,000 requests per minute on average might need to scale to 25,000 during daily peaks and 40,000 during monthly billing cycles. Without accounting for these patterns, you might either over-provision resources (wasting money) or under-provision (risking outages during peak periods).

When time-series data follows known patterns, deviations from these patterns often signal potential problems requiring investigation.



![Diagram: System Metrics with Seasonal Patterns](D:\_TheVault\2_Work\AT-CRS-HTD\01-Data Engineering Course\_week-01\01 Data Literacy\02-statistics-for-interpreting-data\_assets\diagram-system-metrics-with-seasonal-patterns.png)


> **Diagram: System Metrics with Seasonal Patterns**
> 
> This Gantt chart visualizes typical daily patterns in system metrics, showing how different metrics (traffic, CPU usage, database queries) exhibit predictable seasonal patterns throughout a day.





## Understanding Outliers

### Detecting Outliers with Statistical Methods

Outliers—data points that deviate significantly from the rest of the dataset—can provide valuable insights or indicate problems in software systems. Detecting them accurately is crucial for both system monitoring and data analysis.



Common statistical methods for outlier detection include:

**Z-score method**: Identifies values more than n standard deviations from the mean

```python
def z_score_outliers(data, threshold=3):
    mean = sum(data) / len(data)
    std_dev = (sum((x - mean) ** 2 for x in data) / len(data)) ** 0.5
    
    outliers = []
    for value in data:
        z = abs((value - mean) / std_dev)
        if z > threshold:
            outliers.append(value)
    
    return outliers
```



**IQR method**: Identifies values outside 1.5 × the interquartile range

```python
def iqr_outliers(data, k=1.5):
    sorted_data = sorted(data)
    q1, q3 = calculate_quartiles(sorted_data)
    iqr = q3 - q1
    
    lower_bound = q1 - k * iqr
    upper_bound = q3 + k * iqr
    
    return [x for x in data if x < lower_bound or x > upper_bound]
```



**DBSCAN**: Density-based clustering that identifies points in sparse regions as outliers

**Isolation Forest**: Machine learning algorithm that isolates outliers through recursive partitioning

For visualization, box plots clearly highlight outliers by showing data distribution through quartiles. Points plotted beyond the whiskers (typically 1.5 × IQR) are considered outliers:

```
    +-----+     o     o
    |     |
----+     +----
    |     |
    +-----+
```

The choice of outlier detection method should align with your data characteristics and monitoring objectives. Z-scores work well for normally distributed metrics but can be misleading for skewed distributions like response times. For such cases, robust methods like MAD (Median Absolute Deviation) often provide more reliable outlier detection with fewer false positives. Many modern anomaly detection systems employ multiple complementary techniques in parallel, using ensemble approaches to leverage the strengths of different statistical methods.

The scatter plot below represents the distribution of request duration and payload size, with statistical outliers marked in red. Such visualizations help identify unusual combinations that may indicate potential issues.



### Interpreting the Significance of Outliers

Detecting outliers is only the first step; interpreting their significance is crucial. In software contexts, outliers generally fall into three categories:

1. **Measurement errors**: Invalid readings, logging errors, or instrumentation bugs
2. **Legitimate edge cases**: Rare but valid behaviors that deserve attention
3. **Anomalies**: Indicators of potential problems requiring investigation

Consider a case study from a payment processing system where transaction processing times normally range from 200-500ms. The monitoring system flagged five transactions taking over 2000ms in a 10-minute window. Analysis revealed:

- Two transactions: Database locks from concurrent updates (system issue)
- One transaction: Extremely large order with 200+ items (legitimate edge case)
- Two transactions: Network timeouts to a third-party service (external dependency issue)

This example demonstrates why context matters when interpreting outliers. The same statistical deviation can have different implications depending on its cause.

When managing outliers in software systems:

1. **Document normal ranges** for key metrics
2. **Distinguish between types** of outliers through contextual analysis
3. **Prioritize investigation** based on frequency and impact
4. **Design for resilience** against legitimate edge cases
5. **Adjust monitoring thresholds** based on observed patterns

Engineering teams often create specialized dashboards for outlier analysis, automatically enriching anomalous data points with contextual information like associated user segments, geographic regions, or deployment changes. This context-aware approach dramatically reduces investigation time compared to looking at raw statistical anomalies in isolation.

In performance testing, outliers often highlight potential bottlenecks under specific conditions. Rather than dismissing them, use them to understand system behavior at the boundaries of normal operation.



![Diagram: Outlier Analysis Decision Tree](D:\_TheVault\2_Work\AT-CRS-HTD\01-Data Engineering Course\_week-01\01 Data Literacy\02-statistics-for-interpreting-data\_assets\diagram-outlier-analysis-decision-tree.png)




> **Diagram: Outlier Analysis Decision Tree**
> 
> This decision tree helps students understand how to respond to outliers based on their source - whether they're measurement errors, edge cases, or system anomalies. It provides actionable steps for each scenario.



## Correlation vs Causation

### Measuring Correlation

**Correlation** quantifies the degree to which two variables relate to each other. A correlation coefficient ranges from -1 (perfect negative correlation) to +1 (perfect positive correlation), with 0 indicating no correlation.

The most common measure is the Pearson correlation coefficient:
```python
def pearson_correlation(x, y):
    n = len(x)
    # Ensure we have paired data
    assert n == len(y), "Arrays must have the same length"
    
    # Calculate means
    mean_x = sum(x) / n
    mean_y = sum(y) / n
    
    # Calculate covariance and variances
    covar = sum((x[i] - mean_x) * (y[i] - mean_y) for i in range(n))
    var_x = sum((xi - mean_x) ** 2 for xi in x)
    var_y = sum((yi - mean_y) ** 2 for yi in y)
    
    # Calculate correlation
    return covar / (var_x ** 0.5 * var_y ** 0.5)
```

In software engineering, correlations help identify relationships between:
- Response time and concurrent users
- Memory usage and request volume
- Code coverage and defect rates
- Feature usage and user retention

However, correlation strength varies widely in significance. A correlation of 0.9 between CPU usage and response time suggests a strong relationship, while a correlation of 0.2 between page load time and conversion rate represents a weak relationship that might still be economically important.

Beyond linear relationships measured by Pearson's coefficient, other correlation measures like Spearman's rank correlation and Kendall's tau capture monotonic but non-linear relationships. These non-parametric approaches often better represent relationships in complex systems where linear assumptions don't hold. For example, the relationship between database connection pool size and throughput may show a strong Spearman correlation even when the Pearson coefficient is modest, indicating that larger pools generally improve performance even if the relationship isn't strictly linear.

To interpret correlation properly, visualize the data with scatter plots to check for:
- **Linearity**: Does a straight line reasonably represent the relationship?
- **Outliers**: Are a few points driving the correlation?
- **Clusters**: Are there distinct groups showing different relationships?



### Establishing Causation

The critical distinction between correlation and causation can be summarized as: **Correlation indicates variables change together; causation indicates one variable causes the other to change.**

Consider three implementation changes that each correlate with a 20% performance improvement:

1. Changing the database query structure
2. Deploying during a period of lower user traffic
3. Adding more server capacity

Without proper analysis, we might incorrectly attribute the improvement to any of these changes, when perhaps only one (or none) was causal.

To establish causation, we need evidence beyond correlation:

1. **Temporal precedence**: The cause must precede the effect
2. **Theoretical mechanism**: A logical explanation for the relationship
3. **Experimental evidence**: Controlled experiments showing the relationship
4. **Elimination of alternatives**: Ruling out other explanations
5. **Dose-response relationship**: Stronger "doses" of the cause produce stronger effects

The gold standard for establishing causation is **A/B testing** (randomized controlled experiments):
```
1. Randomly divide users/systems into groups
2. Apply change to treatment group only
3. Measure outcomes between groups
4. Analyze statistical significance
5. Conclude causal effect if significant difference exists
```

In complex distributed systems, establishing causation becomes particularly challenging due to emergent properties and complex interactions. Techniques like chaos engineering—deliberately introducing controlled failures—can help establish causal relationships by directly manipulating variables rather than passively observing correlations. Similarly, gradual rollouts with careful monitoring (canary deployments) allow engineers to isolate the causal impact of changes while minimizing risk.

Even with strong experimental evidence, consider the following complicating factors:

- **Confounding variables**: Unaccounted factors influencing both variables
- **Selection bias**: Non-random sampling affecting results
- **Reverse causality**: The assumed effect might actually be the cause
- **Interaction effects**: The relationship depends on other variables

In practice, establishing causation in complex systems often requires multiple lines of evidence. When making architectural decisions based on performance data, seek causal evidence through controlled testing rather than relying solely on observed correlations.



![Diagram: Establishing Causation in Software Systems](D:\_TheVault\2_Work\AT-CRS-HTD\01-Data Engineering Course\_week-01\01 Data Literacy\02-statistics-for-interpreting-data\_assets\diagram-establishing-causation-in-software-systems.png)




> **Diagram: Establishing Causation in Software Systems**
>
> This flowchart outlines the process for determining whether a correlation represents a causal relationship. It demonstrates the systematic approach needed to move from observed correlation to established causation.



## Key Takeaways

- **Choose appropriate analysis methods** based on whether your data is continuous or discrete
- **Use central tendency measures strategically**: mean for normal distributions, median for skewed data
- **Apply probability concepts** to evaluate the significance of observations and test results
- **Analyze time-series data** with awareness of trends, seasonality, and cyclical patterns
- **Interpret outliers** based on context, not just statistical definitions
- **Distinguish correlation from causation** when evaluating relationships between variables



## Conclusion

Statistical literacy transforms raw data into engineering insights. Throughout this lesson, we've explored how to choose the right statistical measures for different data types, detect meaningful patterns in time-series data, identify significant outliers, and avoid the common pitfall of confusing correlation with causation. 

These skills directly enhance your ability to make data-informed decisions about system architecture, performance optimization, and incident response. As we move into our next lesson on data collection and sources, you'll learn how to evaluate data quality, distinguish between different collection methods, and identify potential biases—building upon the statistical foundation established here. 

Remember that statistics in software engineering isn't about mathematical perfection but about making better decisions with the information available to you.

## Glossary

- **Central tendency**: Measures that represent the "middle" of a dataset (mean, median, mode)
- **Confounding variable**: A factor that influences both the independent and dependent variables
- **Correlation coefficient**: A measure of the strength and direction of a relationship between variables
- **Discrete data**: Data that can only take specific values, typically counted
- **Continuous data**: Data that can take any value within a range, typically measured
- **Outlier**: A data point that differs significantly from other observations
- **Sample space**: The set of all possible outcomes of a random process
- **Z-score**: The number of standard deviations a data point is from the mean
