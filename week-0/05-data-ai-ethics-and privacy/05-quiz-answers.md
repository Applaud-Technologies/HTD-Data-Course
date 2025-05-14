# Lesson: Data/AI Ethics and Privacy

### Multiple Choice Questions

**Question 1:**  
What is the primary difference between cognitive biases and statistical biases?

a) Cognitive biases affect only novices, while statistical biases affect experts  
b) Cognitive biases stem from mental processes, while statistical biases come from methodology  
c) Cognitive biases occur in data visualization, while statistical biases occur in data collection  
d) Cognitive biases are mathematical errors, while statistical biases are psychological errors  

**Correct Answer:** **b) Cognitive biases stem from mental processes, while statistical biases come from methodology**  

**Explanation:**  
As the lesson explains, cognitive biases are systematic errors in thinking that affect decisions and judgments (originating in our mental processes), whereas statistical biases are systematic errors in data collection or analysis methodology (structural problems that distort results regardless of who's interpreting them).


**Question 2:**  
Which of the following best represents sampling bias?

a) A data scientist favoring evidence that supports their existing hypothesis  
b) Claiming that an event was predictable after it already occurred  
c) A fitness app study where 90% of participants use newer smartphones  
d) Heavily weighting the first piece of information received in an analysis  

**Correct Answer:** **c) A fitness app study where 90% of participants use newer smartphones**  

**Explanation:**  
Sampling bias occurs when your dataset doesn't accurately represent the population you're trying to study. The fitness app example from the lesson demonstrates this perfectly - the data predominantly represented tech-savvy users with newer smartphones, not the general population, creating a systematic sampling error.


**Question 3:**  
At which stage of the data lifecycle is bias MOST difficult to correct once introduced?

a) Analysis stage  
b) Collection stage  
c) Processing stage  
d) Reporting stage  

**Correct Answer:** **b) Collection stage**  

**Explanation:**  
The lesson states that collection represents the "original sin from which other biases often flow" and that "once data is collected with inherent biases, downstream analyses can rarely fully compensate." This makes collection the most critical stage for bias prevention.


**Question 4:**  
Which legal consideration should be evaluated FIRST when planning to scrape data from a website?

a) Differential privacy requirements  
b) The website's robots.txt file and terms of service  
c) Whether synthetic data could be used instead  
d) If the data will be used for commercial purposes  

**Correct Answer:** **b) The website's robots.txt file and terms of service**  

**Explanation:**  
The lesson lists reviewing the site's robots.txt file and terms of service as the first key consideration before scraping data. This is the initial step to determine if scraping is permitted by the site owner and what restrictions might apply.


**Question 5:**  
What is a key principle of ethical data visualization?

a) Always using logarithmic scales to emphasize differences  
b) Removing outliers to create cleaner visualizations  
c) Using proper scaling that neither exaggerates nor minimizes differences  
d) Simplifying complex data by removing confidence intervals  

**Correct Answer:** **c) Using proper scaling that neither exaggerates nor minimizes differences**  

**Explanation:**  
The lesson emphasizes that ethical visualization requires "proper scaling" that neither exaggerates nor minimizes differences, including using zero-baseline charts for magnitude comparisons and consistent scales when comparing groups to prevent misleading impressions.


### Multiple Answer Questions

**Question 6:**  
Which of the following are examples of cognitive biases mentioned in the lesson? (Select all that apply)

a) Confirmation bias  
b) Sampling bias  
c) Hindsight bias  
d) Anchoring bias  
e) Observer bias  

**Correct Answers:**  
**a) Confirmation bias**  
**c) Hindsight bias**  
**d) Anchoring bias**  

**Explanation:**  
The lesson specifically identifies confirmation bias (favoring information that confirms existing beliefs), hindsight bias (seeing past events as predictable), and anchoring bias (relying too heavily on first information received) as cognitive biases. Sampling bias and observer bias are classified as statistical biases.


**Question 7:**  
Which of the following are statistical biases mentioned in the lesson? (Select all that apply)

a) Anchoring bias  
b) Sampling bias  
c) Survivorship bias  
d) Seasonal bias  
e) Confirmation bias  

**Correct Answers:**  
**b) Sampling bias**  
**c) Survivorship bias**  
**d) Seasonal bias**  

**Explanation:**  
The lesson identifies sampling bias (unrepresentative samples), survivorship bias (focusing only on things that "survived" a selection process), and seasonal bias (when periodic patterns affect data collection) as statistical biases. Anchoring bias and confirmation bias are cognitive biases.


**Question 8:**  
Which criteria should be considered when evaluating data source credibility? (Select all that apply)

a) Source authority  
b) Methodology transparency  
c) Potential conflicts of interest  
d) Visual appeal of the data  
e) Sample representation  

**Correct Answers:**  
**a) Source authority**  
**b) Methodology transparency**  
**c) Potential conflicts of interest**  
**e) Sample representation**  

**Explanation:**  
The lesson's Credibility Assessment Framework includes source authority (institutional reputation, expertise), methodology transparency (documented collection process), conflict of interest (funding sources, collection motivations), and sample representation (demographic coverage, selection methods). Visual appeal is not mentioned as a credibility criterion.


**Question 9:**  
Which of the following are privacy-preserving approaches mentioned in the lesson? (Select all that apply)

a) Data anonymization  
b) Differential privacy  
c) Synthetic data  
d) Federated learning  
e) P-hacking  

**Correct Answers:**  
**a) Data anonymization**  
**b) Differential privacy**  
**c) Synthetic data**  
**d) Federated learning**  

**Explanation:**  
The lesson discusses data anonymization (removing identifying information), differential privacy (adding calibrated noise), synthetic data (generating artificial records), and federated learning (keeping data on local devices) as privacy-preserving approaches. P-hacking is mentioned as an unethical practice related to model selection bias.


**Question 10:**  
Which of the following are ethical practices for communicating data limitations? (Select all that apply)

a) Providing explicit confidence metrics  
b) Describing sample characteristics clearly  
c) Acknowledging alternative interpretations  
d) Using logarithmic scales without explanation  
e) Disclosing technical debt in analysis  

**Correct Answers:**  
**a) Providing explicit confidence metrics**  
**b) Describing sample characteristics clearly**  
**c) Acknowledging alternative interpretations**  
**e) Disclosing technical debt in analysis**  

**Explanation:**  
The lesson specifically mentions including explicit confidence metrics with predictions, clearly describing sample characteristics, acknowledging alternative interpretations of data, and disclosing technical debt (compromises made during analysis) as ethical data communication practices. Using logarithmic scales without explanation would be considered misleading visualization.