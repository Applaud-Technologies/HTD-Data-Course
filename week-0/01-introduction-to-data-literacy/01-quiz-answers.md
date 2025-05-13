# Lesson: Introduction to Data Literacy

### Multiple Choice Questions

**Question 1:**  
What does data literacy operationally mean for engineers?

a) The ability to perform complex statistical analyses  
b) The ability to build data warehouses and lakes  
c) The ability to read, write, and communicate data in context  
d) The ability to code machine learning algorithms  

**Correct Answer:** **c) The ability to read, write, and communicate data in context**  

**Explanation:**  
As defined in the lesson, data literacy for engineers operationally means interpreting dashboards and metrics (reading data), designing systems that collect appropriate telemetry (writing data), and translating data insights into engineering actions (communicating data).


**Question 2:**  
Which of the following best describes Metric-Driven Development (MDD)?

a) A methodology focused exclusively on performance metrics  
b) A development approach that starts with measurable success metrics before implementing features  
c) A process that prioritizes metrics over user experience  
d) A technique for optimizing database performance  

**Correct Answer:** **b) A development approach that starts with measurable success metrics before implementing features**  

**Explanation:**  
Metric-Driven Development (MDD) establishes measurable outcomes before implementing features, similar to how Test-Driven Development defines success criteria before writing code. MDD involves specifying success metrics, instrumenting for those metrics, shipping with A/B testing, and using data to inform iterations.


**Question 3:**  
Which of the following represents the primary transition point from conventional data to Big Data?

a) When data requires more than one analyst to interpret  
b) When data processing requires machine learning  
c) When existing tools and approaches break down due to scale and complexity  
d) When data reaches exactly 1 terabyte in size  

**Correct Answer:** **c) When existing tools and approaches break down due to scale and complexity**  

**Explanation:**  
The transition from conventional to Big Data is less about arbitrary size thresholds and more about when your existing tools and approaches break down. It's similar to how monolithic applications eventually reach a complexity point where microservices architectures become necessary.


**Question 4:**  
What distinguishes truly data-driven organizations from those that merely collect data?

a) The amount of data they collect  
b) Their use of AI and machine learning  
c) Their implementation of a closed feedback loop that validates insights with measured impacts  
d) The size of their data science team  

**Correct Answer:** **c) Their implementation of a closed feedback loop that validates insights with measured impacts**  

**Explanation:**  
Truly data-driven organizations close the feedback loop by measuring the impact of changes implemented based on data insights. This scientific approach to feature development—forming hypotheses, testing them with real user data, and iterating based on results—distinguishes them from organizations that collect data without acting on it.


**Question 5:**  
In the context of data literacy, what is a "data contract"?

a) A legal agreement about data ownership  
b) An agreement between teams specifying what data will be collected, how it will be structured, and how it can be accessed  
c) A service level agreement for data accuracy  
d) A document describing data privacy policies  

**Correct Answer:** **b) An agreement between teams specifying what data will be collected, how it will be structured, and how it can be accessed**  

**Explanation:**  
Data contracts are agreements between teams that specify what data each team will collect, how it will be structured, and how it can be accessed. Similar to API contracts, they ensure that cross-functional metrics can be calculated consistently, creating a shared source of truth.


### Multiple Answer Questions

**Question 6:**  
Which of the following are identified as characteristics of a data-literate engineer? (Select all that apply)

a) Ability to independently query and interpret metrics without waiting for analysts  
b) Skill in designing systems that collect appropriate telemetry  
c) Capability to translate data insights into specific code changes  
d) Advanced degree in statistics  

**Correct Answers:**  
**a) Ability to independently query and interpret metrics without waiting for analysts**  
**b) Skill in designing systems that collect appropriate telemetry**  
**c) Capability to translate data insights into specific code changes**  

**Explanation:**  
Data-literate engineers can independently query conversion rates and metrics (a), design systems that collect appropriate telemetry and structure it logically (b), and recommend specific code changes based on data analysis (c). While statistical knowledge is valuable, an advanced degree (d) is not required for engineering data literacy.


**Question 7:**  
Which of the following are components of effective data infrastructure in high-performing teams? (Select all that apply)

a) Shared, self-service dashboards  
b) API-first data architecture  
c) Common data dictionary  
d) Centralized data access controlled by a single team  

**Correct Answers:**  
**a) Shared, self-service dashboards**  
**b) API-first data architecture**  
**c) Common data dictionary**  

**Explanation:**  
Effective data infrastructure includes shared, self-service dashboards that don't require analysts to build reports (a), API-first data architecture that makes data accessible via well-documented internal APIs (b), and a common data dictionary that standardizes metric definitions (c). Centralized access controlled by a single team (d) contradicts the principle of democratizing data access.


**Question 8:**  
Which of the following are part of the 10 Vs framework for Big Data? (Select all that apply)

a) Volume  
b) Velocity  
c) Visibility  
d) Veracity  

**Correct Answers:**  
**a) Volume**  
**b) Velocity**  
**d) Veracity**  

**Explanation:**  
The 10 Vs framework includes Volume (data size requiring distributed storage), Velocity (speed of data processing), and Veracity (uncertainty/reliability of data). Visibility is not one of the 10 Vs in the framework, which actually includes: Volume, Velocity, Variety, Veracity, Value, Validity, Variability, Venue, Vocabulary, and Vagueness.


**Question 9:**  
Which organizational roles are typically found in data-driven companies? (Select all that apply)

a) Chief Data Officer (CDO)  
b) Embedded Data Engineers  
c) Data Stewards  
d) Data Gatekeepers  

**Correct Answers:**  
**a) Chief Data Officer (CDO)**  
**b) Embedded Data Engineers**  
**c) Data Stewards**  

**Explanation:**  
Data-driven organizations typically have a Chief Data Officer focused on data strategy and governance (a), Embedded Data Engineers within engineering teams (b), and Data Stewards who are subject-matter experts owning data quality in their domain (c). "Data Gatekeepers" contradicts the philosophy of democratizing data access in data-driven organizations.


**Question 10:**  
Which stages are part of the data lifecycle? (Select all that apply)

a) Generation  
b) Collection  
c) Visualization  
d) Monetization  

**Correct Answers:**  
**a) Generation**  
**b) Collection**  
**c) Visualization**  

**Explanation:**  
The data lifecycle includes Generation (data creation from user actions, logs, etc.), Collection (capturing and storing raw data), and Visualization (representing findings in human-interpretable formats). While data might be monetized in some business contexts, Monetization is not defined as a standard stage in the data lifecycle as presented in the lesson.
