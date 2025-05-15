# Student Guide: Data Collection & Sources

## Overview

This lesson explores the foundational skills for sourcing, collecting, and evaluating data—skills critical for engineers in data-rich environments like finance, insurance, and fintech. You'll learn to assess data origin, structure, quality, and bias, supporting more reliable downstream analysis and machine learning outcomes.

## Core Learning Outcomes (Simplified for Reference)

- Differentiate primary and secondary data sources in engineering contexts.
- Apply appropriate data collection techniques for varied use cases.
- Use sampling strategies to create representative subsets of large datasets.
- Evaluate data quality using standardized frameworks and metrics.
- Detect and mitigate data collection biases to preserve analytical integrity.
- Match structured, semi-structured, and unstructured data formats to analysis needs.

## Key Vocabulary

| Term | Definition |
|------|------------|
| **Primary Data** | Data collected directly for a specific purpose or analysis. |
| **Secondary Data** | Pre-existing data originally collected for other purposes. |
| **Sampling** | Selecting a subset of data to represent a larger population. |
| **Stratified Sampling** | Dividing population into groups and sampling within each. |
| **Bias** | Systematic error introduced during data collection or analysis. |
| **Accuracy** | Degree to which data reflects the real-world values. |
| **Completeness** | Extent to which required data points are present. |
| **Provenance** | Documentation of a dataset’s origin and transformation history. |
| **Structured Data** | Tabular data with predefined schema (e.g., SQL tables). |
| **Semi-Structured Data** | Flexible data with tags or markers (e.g., JSON, XML). |
| **Unstructured Data** | Data without formal structure (e.g., text, images). |
| **Sensor Data** | Time-series signals collected via IoT devices or monitoring tools. |
| **Crowdsourcing** | Data collected from distributed voluntary contributors. |
| **Web Scraping** | Automated extraction of data from web pages. |
| **API Integration** | Accessing third-party data through structured endpoints. |

## Core Concepts & Takeaways

### Primary vs Secondary Data
- **Primary** = custom-built datasets, more relevant but costlier.
- **Secondary** = reused data, faster but with limitations.
- Balance specificity vs. efficiency, much like build vs. buy decisions in dev work.

### Data Collection Methods
| Method | Use Case |
|--------|----------|
| Surveys | Human-perception questions, limited scale |
| APIs | Structured third-party systems |
| Scraping | No API access, public content |
| Sensors | Continuous measurement, real-time |
| Logs | Event streams, system monitoring |
| Databases | Existing systems with transactional data |

Select based on: human vs. system input, frequency needs, data control, and ethics.

### Sampling Techniques
| Type | Description |
|------|-------------|
| Random | Equal chance for all data points |
| Stratified | Group-wise proportional sampling |
| Cluster | Entire subgroups are sampled |
| Systematic | Every nth element after a start point |

Used to balance performance and representativeness, especially in testing and experimentation.

### Data Quality Assessment
Eight dimensions: Accuracy, Completeness, Consistency, Timeliness, Validity, Uniqueness, Provenance, Relevance.

Use metrics like:
- Null Ratio
- Freshness Decay
- Outlier Proportion
- Duplicate Ratio
- Schema Conformity

Automation and alerting are essential for scale.

### Bias in Data Collection
| Bias Type | Example |
|-----------|---------|
| Selection | Only active users are sampled |
| Survivorship | Failed cases excluded |
| Confirmation | Collection favors expected outcomes |
| Measurement | Inaccurate sensors or self-reporting |
| Automation | Overreliance on flawed pipelines |

Mitigation strategies: documentation, stratification, sensitivity analysis, and benchmarking.

### Data Structure Types
| Type | Example | Best Use |
|------|---------|----------|
| Structured | SQL Tables | Fast querying |
| Semi-Structured | JSON, XML | Flexible schemas |
| Unstructured | Text, audio | NLP or image analysis |
| Time-Series | Monitoring data | Trend and pattern detection |
| Graph | Social networks | Relationship modeling |

Select based on performance needs, analysis goals, and schema evolution.

## Tools & Technologies Mentioned

| Tool / Concept | Usage Context |
|----------------|----------------|
| **Python** | Logic for sampling decisions and data validation |
| **APIs** | Access to structured external data |
| **Sensor Networks** | Real-time environmental data collection |
| **Crowdsourcing Platforms** | Collect voluntary distributed data |
| **Databases** | Structured, transactional data sources |
| **Web Scraping** | Extracting data from websites |

## Career Alignment

Understanding data sources, sampling, structure, and bias ensures you're not just coding for analysis—but building pipelines that produce valid, usable insights. These skills enable data engineers to design resilient systems, reduce error, and deliver reliable inputs for high-stakes decisions in regulated, high-volume industries.
