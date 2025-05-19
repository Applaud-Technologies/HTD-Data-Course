# Ethical Considerations in Financial Data

The financial services industry thrives on data. From underwriting loans to recommending investment portfolios, decisions that once depended on human judgment are now increasingly driven by algorithms, machine learning (ML), and vast streams of structured and unstructured data. While this transformation promises efficiency and scale, it also amplifies long-standing ethical challenges and introduces new risks that demand urgent attention.

## The Evolution of Ethical Walls in Finance

The concept of an internal information barrier—commonly referred to as an "ethical wall"—originated in the aftermath of the 1929 stock market crash. In response to widespread abuses, Congress enacted the Glass-Steagall Act in 1933, requiring the separation of commercial and investment banking activities to prevent conflicts of interest¹. This led to the institutionalization of barriers that prohibited the flow of non-public information between departments. The goal was to prevent scenarios where, for example, investment bankers working on an IPO could influence or coordinate with analysts recommending that IPO to retail investors.

Over the years, the ethical wall has been tested repeatedly—during the dotcom boom, the 2007–08 financial crisis, and the rise of data-driven investment strategies. Regulatory reforms, such as the Insider Trading and Securities Fraud Enforcement Act of 1988 and post-dotcom crash settlements, mandated stricter divisions between research and investment banking². Yet history shows that ethical boundaries are only as strong as the incentives and oversight mechanisms behind them.

## Core Ethical Principles for Financial Data Use

The financial sector is bound by legal and professional obligations to protect clients and ensure fairness in markets. Ethical behavior is not optional—it underpins the trust upon which financial systems operate. Professional organizations and regulators emphasize four key ethical principles:

1. **Integrity** – Avoiding deceptive practices and misrepresentation of data or results.
2. **Transparency** – Ensuring algorithms, data sources, and decision-making criteria are explainable.
3. **Accountability** – Assigning responsibility for the design, deployment, and consequences of data systems.
4. **Confidentiality** – Protecting sensitive financial and personal data from misuse or unauthorized access³.

These principles must be embedded into the work of data engineers who design pipelines, implement ML models, and develop analytics infrastructures in finance.

## Ethical Dilemmas in Practice

### Conflicts of Interest

Financial analysts and advisors may be incentivized to favor certain products or clients due to internal compensation structures, even when these recommendations conflict with a client’s best interest⁴. In data systems, this manifests in feature engineering, scoring logic, and recommendation engines that may skew results in favor of internal business goals rather than user outcomes.

### Insider Trading and Data Access

Access to material non-public information (MNPI) is tightly regulated. Engineers must design systems that include role-based access controls, logging, and isolation between data domains to prevent improper data exposure⁵.

### Bias in AI and Machine Learning

ML systems trained on historical financial data may inherit and perpetuate systemic biases. Amazon famously abandoned a recruitment algorithm after it was found to downgrade resumes from women because it learned from a male-dominated hiring history⁶. In the U.S. mortgage market, Black and Latinx borrowers continue to face algorithmic discrimination in loan approvals and interest rates, even in supposedly automated systems⁷.

Bias in financial models may not only produce unethical outcomes—it may also be illegal under anti-discrimination laws and financial regulations.

### Inadequate Anonymization

Data that is believed to be anonymized can often be reverse-engineered. Research has shown that 90% of individuals can be re-identified from anonymized credit card data using just four spatiotemporal points⁸. For financial institutions that sell or share aggregated data, this creates serious privacy risks, regulatory exposure, and reputational harm.

### Questionable Research Practices

In model development, the temptation to "HARK"—Hypothesize After Results are Known—can lead to survivor bias. Like mutual funds that quietly retire underperforming products, AI models may appear successful if failed experiments are never disclosed. Without transparency into model lineage, data provenance, and the experimental process, results may be untrustworthy⁹.

### Incomplete or Unrepresentative Data

ML systems often fail to reflect the full diversity of real-world populations. For example, speech and facial recognition systems have been shown to perform poorly for Black users due to underrepresentation in training data¹⁰. In finance, reliance on biased or incomplete data may exclude marginalized groups from fair access to loans, investments, or insurance.

### Misuse of AI in High-Stakes Decisions

The deployment of AI in sensitive areas—credit scoring, fraud detection, or financial advising—demands careful ethical review. As the 2020 UK exam results fiasco demonstrated, automating complex social judgments with algorithms can lead to widespread harm when transparency and oversight are lacking¹¹. Financial firms must assess whether automation offers actual benefits or simply introduces new risks without improving outcomes.

## Regulatory Momentum and the Need for Governance

With AI/ML expanding across financial services, regulators are responding. The Bank of England and Financial Conduct Authority have both expressed concern that traditional model governance frameworks are insufficient to manage AI-specific risks¹². Ethical failures in finance are not just PR disasters—they carry **legal liabilities, regulatory fines, and erosion of public trust**.

To mitigate these risks, financial institutions should:

* Assign senior executive accountability for AI/ML oversight.
* Establish ethics boards that include external stakeholders.
* Implement transparent model validation and auditing.
* Maintain detailed documentation of model development decisions.
* Use fairness metrics and bias detection tools during model training and deployment.

## Conclusion

Ethical considerations in financial data are no longer an abstract or future-facing issue. They are present, material, and increasingly scrutinized by regulators, journalists, and the public. Data engineers who work in finance are not just technical implementers—they are stewards of systems that affect access to credit, opportunities, and financial well-being.

Ethics, like security, must be built into systems from the ground up. As the financial sector becomes more automated and data-driven, the ability to design with integrity will define not just professional credibility—but public trust.

---

### References

1. Lisa Smith, “How the Ethical Wall Works on Wall Street,” *Investopedia*, January 14, 2025. [https://www.investopedia.com/articles/analyst/090501.asp](https://www.investopedia.com/articles/analyst/090501.asp)
2. Ibid.
3. “Professional Ethics in Finance and Analytics,” Santa Clara University Leavey School of Business, March 18, 2024. [https://onlinedegrees.scu.edu/media/blog/professional-ethics-in-finance-and-analytics](https://onlinedegrees.scu.edu/media/blog/professional-ethics-in-finance-and-analytics)
4. Sruthi K., “Ethical Issues and Dilemmas in Financial Analysis,” *LinkedIn*, July 5, 2024. [https://www.linkedin.com/pulse/ethical-issues-dilemmas-financial-analysis-sruthi-kakarlapudi-at2oc/](https://www.linkedin.com/pulse/ethical-issues-dilemmas-financial-analysis-sruthi-kakarlapudi-at2oc/)
5. Ibid.
6. Reuters, “Amazon scraps secret AI recruiting tool that showed bias against women,” 2018.
7. Bartlett, Morse et al., “Consumer-Lending Discrimination in the FinTech Era,” 2019. [http://faculty.haas.berkeley.edu/morse/research/papers/discrim.pdf](http://faculty.haas.berkeley.edu/morse/research/papers/discrim.pdf)
8. de Montjoye et al., “Unique in the shopping mall: On the reidentifiability of credit card metadata,” *Science*, 2015.
9. Tim Jennings, *Data Ethics and the Use of AI/ML in Financial Services*, Synechron, 2023.
10. “Racial Disparities in Automated Speech Recognition,” *PNAS*, 2020.
11. Forbes, “UK Exam Results U-Turn: Algorithms Alone Can’t Solve Complex Human Problems,” 2020.
12. Bank of England & FCA, “DP5/22 - Artificial Intelligence and Machine Learning,” October 2022.
