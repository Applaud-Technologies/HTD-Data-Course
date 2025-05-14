# Lesson: Detecting and Mitigating Bias

### Multiple Choice Questions

**Question 1:**  
Which of the following best describes algorithmic bias in AI systems?

a) Human judgment errors that occur during data selection and interpretation  
b) Systematic errors in sampling that cause expected values to deviate from true population values  
c) Systematic differences in model outputs for different demographic groups  
d) Cognitive errors made by model developers during feature engineering  

**Correct Answer:** **c) Systematic differences in model outputs for different demographic groups**  

**Explanation:**  
Algorithmic bias occurs when a model produces systematically different outputs for different groups, often amplifying historical inequities. This differs from cognitive bias (human judgment) and statistical bias (sampling errors).

**Question 2:**  
Which statistical test is most appropriate for assessing if categorical distributions in your sample match expected population distributions?

a) K-S test  
b) Chi-square test  
c) ANOVA  
d) Linear regression  

**Correct Answer:** **b) Chi-square test**  

**Explanation:**  
The Chi-square test is specifically designed to assess if categorical distributions in your sample match expected population distributions. K-S tests are more appropriate for continuous distributions.

**Question 3:**  
What does the fairness metric "Equal Opportunity" measure?

a) Equal positive prediction rates across groups  
b) Equal true positive rates across groups  
c) Equal false positive rates across groups  
d) Equal positive predictive values across groups  

**Correct Answer:** **b) Equal true positive rates across groups**  

**Explanation:**  
Equal Opportunity is a fairness metric that requires equal true positive rates across demographic groups, mathematically expressed as P(Ŷ=1|Y=1,A=a) = P(Ŷ=1|Y=1,A=b) for groups a and b.

**Question 4:**  
Which bias mitigation approach adjusts prediction thresholds differently for various demographic groups to achieve fairness?

a) Balanced sampling  
b) Adversarial debiasing  
c) Threshold adjustment  
d) Feature transformation  

**Correct Answer:** **c) Threshold adjustment**  

**Explanation:**  
Threshold adjustment is a post-processing technique that uses different decision thresholds for different groups to equalize error rates. This approach can be applied to existing models without retraining.

**Question 5:**  
In the bias propagation chain, what typically causes statistical bias?

a) Algorithmic limitations  
b) Cognitive assumptions  
c) Model architecture  
d) Hardware constraints  

**Correct Answer:** **b) Cognitive assumptions**  

**Explanation:**  
In the bias propagation chain, cognitive assumptions and biases lead to statistical biases in data collection methods, which then lead to algorithmic biases in models. Human cognitive biases are often the root cause of downstream statistical biases.

### Multiple Answer Questions

**Question 6:**  
Which of the following are types of bias discussed in the lesson? (Select all that apply)

a) Cognitive bias  
b) Statistical bias  
c) Algorithmic bias  
d) Structural bias  

**Correct Answers:**  
**a) Cognitive bias**  
**b) Statistical bias**  
**c) Algorithmic bias**  

**Explanation:**  
The lesson specifically discusses three main types of bias: cognitive bias (originating in human judgment), statistical bias (systematic errors in sampling or estimation), and algorithmic bias (systematic differences in model outputs for different groups). Structural bias wasn't explicitly categorized in this taxonomy.

**Question 7:**  
Which techniques can be used to detect sampling bias in datasets? (Select all that apply)

a) Chi-square tests  
b) Kolmogorov-Smirnov tests  
c) Propensity scoring  
d) Gradient boosting  

**Correct Answers:**  
**a) Chi-square tests**  
**b) Kolmogorov-Smirnov tests**  
**c) Propensity scoring**  

**Explanation:**  
Chi-square tests assess categorical distributions, K-S tests determine if continuous distributions differ significantly, and propensity scoring identifies over/under-represented samples. These are all valid techniques for detecting sampling bias. Gradient boosting is a machine learning algorithm, not a bias detection method.

**Question 8:**  
Which of the following are valid pre-processing bias mitigation techniques? (Select all that apply)

a) Balanced sampling  
b) Reweighting  
c) Feature transformation  
d) Threshold adjustment  

**Correct Answers:**  
**a) Balanced sampling**  
**b) Reweighting**  
**c) Feature transformation**  

**Explanation:**  
Balanced sampling (over/under-sampling), reweighting (assigning importance weights), and feature transformation (removing correlations between sensitive attributes and other features) are all pre-processing bias mitigation techniques. Threshold adjustment is a post-processing technique.

**Question 9:**  
Which fairness metrics focus on equalizing error rates across demographic groups? (Select all that apply)

a) Demographic Parity  
b) Equal Opportunity  
c) Equalized Odds  
d) Predictive Parity  

**Correct Answers:**  
**b) Equal Opportunity**  
**c) Equalized Odds**  

**Explanation:**  
Equal Opportunity equalizes true positive rates across groups, while Equalized Odds equalizes both true positive and false positive rates across groups. Both focus on error rate equalization. Demographic Parity focuses on equal positive prediction rates, and Predictive Parity focuses on equal positive predictive values.

**Question 10:**  
Which approaches are classified as in-processing bias mitigation techniques? (Select all that apply)

a) Adversarial debiasing  
b) Fairness constraints  
c) Threshold adjustment  
d) Balanced sampling  

**Correct Answers:**  
**a) Adversarial debiasing**  
**b) Fairness constraints**  

**Explanation:**  
Adversarial debiasing and fairness constraints are in-processing techniques that address bias during model training. Threshold adjustment is a post-processing approach, and balanced sampling is a pre-processing technique. In-processing techniques modify the learning algorithm itself.