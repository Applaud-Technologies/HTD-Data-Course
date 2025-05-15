# Quiz: Statistics for Interpreting Data

## Multiple Choice Questions

**Question 1:**  
Which measure of central tendency is most appropriate for analyzing response times in a system with occasional spikes?

a) Mean  
b) Median  
c) Mode  
d) Z-score  

**Correct Answer:** **b) Median**  

**Explanation:**  
The median is more resistant to outliers than the mean, making it better suited for analyzing response times which typically have a skewed distribution with occasional spikes. As noted in the lesson, "for metrics like response times or resource utilization, the median often provides more reliable insights than the mean."

**Question 2:**  
In a normal distribution, approximately what percentage of values fall within ±2 standard deviations of the mean?

a) 68%  
b) 95%  
c) 99.7%  
d) 50%  

**Correct Answer:** **b) 95%**  

**Explanation:**  
For a normal distribution, approximately 95% of values fall within ±2 standard deviations of the mean. This property is useful for establishing normal operating ranges and detecting anomalies in system monitoring.

**Question 3:**  
What is the primary distinction between correlation and causation?

a) Correlation is stronger than causation  
b) Causation is always indicated by strong correlation  
c) Correlation indicates variables change together; causation indicates one variable causes the other to change  
d) Correlation applies to continuous data while causation applies to discrete data  

**Correct Answer:** **c) Correlation indicates variables change together; causation indicates one variable causes the other to change**  

**Explanation:**  
The key distinction is that correlation merely shows that two variables change together, while causation establishes that one variable directly influences or causes the other to change. As the lesson states, establishing causation requires evidence beyond correlation, such as temporal precedence and experimental evidence.

**Question 4:**  
Which statistical method identifies outliers by finding values more than a certain number of standard deviations from the mean?

a) IQR method  
b) Z-score method  
c) DBSCAN  
d) Pearson coefficient  

**Correct Answer:** **b) Z-score method**  

**Explanation:**  
The Z-score method identifies outliers by calculating how many standard deviations a data point is from the mean. Values exceeding a threshold (commonly 3) are considered outliers. This is directly described in the lesson's section on outlier detection methods.

**Question 5:**  
What is the correct formula for calculating the Pearson correlation coefficient?

a) r = covariance(x,y) / (standard deviation of x × standard deviation of y)  
b) r = sum(x × y) / n  
c) r = sum(xi - mean_x) / n  
d) r = (upper quartile - lower quartile) / 2  

**Correct Answer:** **a) r = covariance(x,y) / (standard deviation of x × standard deviation of y)**  

**Explanation:**  
The Pearson correlation coefficient is calculated as the covariance of the two variables divided by the product of their standard deviations. This formula is consistent with the code example provided in the lesson's section on measuring correlation.

## Multiple Answer Questions

**Question 6:**  
Which of the following are characteristics of discrete data? (Select all that apply)

a) Consists of finite, countable values  
b) Can be subdivided into any fractional value  
c) Typically visualized using bar charts  
d) Examples include number of HTTP requests and server restarts  

**Correct Answers:**  
**a) Consists of finite, countable values**  
**c) Typically visualized using bar charts**  
**d) Examples include number of HTTP requests and server restarts**  

**Explanation:**  
Discrete data consists of finite, countable values that cannot be meaningfully subdivided. It is typically visualized using bar charts and frequency tables. Examples mentioned in the lesson include the number of HTTP requests to an API and server restarts. Continuous data, not discrete data, can be subdivided into fractional values.

**Question 7:**  
What are valid methods for establishing causation beyond correlation? (Select all that apply)

a) Temporal precedence (cause precedes effect)  
b) Experimental evidence from controlled experiments  
c) Strong correlation coefficient (r > 0.9)  
d) Elimination of alternative explanations  

**Correct Answers:**  
**a) Temporal precedence (cause precedes effect)**  
**b) Experimental evidence from controlled experiments**  
**d) Elimination of alternative explanations**  

**Explanation:**  
To establish causation, the lesson identifies several necessary elements beyond correlation: temporal precedence (the cause must precede the effect), experimental evidence from controlled experiments, and elimination of alternative explanations. A strong correlation coefficient alone does not establish causation, regardless of its strength.

**Question 8:**  
Which patterns might be observed in time-series data from software systems? (Select all that apply)

a) Daily patterns (higher traffic during business hours)  
b) Weekly patterns (reduced weekend activity)  
c) Random patterns (completely unpredictable behavior)  
d) Quarterly patterns (business reporting cycles)  

**Correct Answers:**  
**a) Daily patterns (higher traffic during business hours)**  
**b) Weekly patterns (reduced weekend activity)**  
**d) Quarterly patterns (business reporting cycles)**  

**Explanation:**  
The lesson specifically mentions several common patterns in software systems: daily patterns (higher traffic during business hours), weekly patterns (reduced weekend activity), and quarterly patterns (business reporting cycles). While systems may have some random variation, completely unpredictable behavior is not listed as a common pattern type.

**Question 9:**  
When interpreting outliers in software systems, which of the following categories do they typically fall into? (Select all that apply)

a) Measurement errors  
b) Legitimate edge cases  
c) System anomalies requiring investigation  
d) Statistical irrelevancies that should be ignored  

**Correct Answers:**  
**a) Measurement errors**  
**b) Legitimate edge cases**  
**c) System anomalies requiring investigation**  

**Explanation:**  
According to the lesson, outliers in software contexts generally fall into three categories: measurement errors (invalid readings or instrumentation bugs), legitimate edge cases (rare but valid behaviors), and anomalies (indicators of potential problems requiring investigation). The lesson emphasizes interpreting outliers based on context rather than dismissing them as irrelevant.

**Question 10:**  
Which visualization methods are appropriate for continuous data? (Select all that apply)

a) Histograms  
b) Density plots  
c) Bar charts  
d) Distribution plots with smooth curves  

**Correct Answers:**  
**a) Histograms**  
**b) Density plots**  
**d) Distribution plots with smooth curves**  

**Explanation:**  
For continuous data, the lesson recommends histograms, density plots, and distribution plots with smooth curves as appropriate visualization methods. Bar charts are specifically mentioned as being more suitable for discrete data because they emphasize the distinct, countable nature of those values.