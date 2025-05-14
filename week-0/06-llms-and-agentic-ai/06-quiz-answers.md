# Lesson: LLMs and Agentic AI

### Multiple Choice Questions

**Question 1:**  
What fundamental shift does the transformer architecture represent compared to traditional models like RNNs?

a) Moving from parallel to sequential processing  
b) Moving from sequential to parallel processing  
c) Moving from supervised to unsupervised learning  
d) Moving from symbolic to neural networks  

**Correct Answer:** **b) Moving from sequential to parallel processing**  

**Explanation:**  
Traditional sequential models like RNNs process text one token at a time, while transformers process entire sequences simultaneously through self-attention mechanisms, enabling them to capture relationships between distant elements more effectively.

**Question 2:**  
Using the formula for calculating an LLM's memory footprint, approximately how much memory would a 13B parameter model in FP16 precision require just for storing weights?

a) 13GB  
b) 26GB  
c) 52GB  
d) 104GB  

**Correct Answer:** **b) 26GB**  

**Explanation:**  
Using the formula: model_size_in_GB = (num_parameters * precision_in_bytes) / (1024^3), we get (13,000,000,000 parameters * 2 bytes) / (1024^3) ≈ 26GB for storing the model weights in FP16 precision.

**Question 3:**  
What is the primary benefit of domain-specific LLMs like BloombergGPT compared to general-purpose models?

a) They require less computational resources  
b) They can understand multiple languages better  
c) They demonstrate superior performance on domain-specific tasks  
d) They generate text faster than general-purpose models  

**Correct Answer:** **c) They demonstrate superior performance on domain-specific tasks**  

**Explanation:**  
Domain-specific models like BloombergGPT are trained on specialized datasets (financial data in this case), enabling them to understand terminology and concepts specific to that domain with higher accuracy than general-purpose alternatives.

**Question 4:**  
Which of the following is NOT identified as a key component of effective prompts?

a) Clear instructions  
b) Relevant context  
c) Format specification  
d) Error handling  

**Correct Answer:** **d) Error handling**  

**Explanation:**  
According to the lesson, effective prompts typically contain three key components: clear instructions, relevant context, and format specification. Error handling is a software development concept but not identified as a core prompt component.

**Question 5:**  
What is the primary purpose of Retrieval-Augmented Generation (RAG) in LLM applications?

a) To increase generation speed  
b) To reduce model size  
c) To ground responses in external knowledge sources  
d) To improve prompt engineering techniques  

**Correct Answer:** **c) To ground responses in external knowledge sources**  

**Explanation:**  
RAG combines parametric knowledge (information in model weights) with non-parametric knowledge (external data accessed at runtime), allowing LLMs to ground their responses in specific datasets or documentation rather than relying solely on pre-trained knowledge.

### Multiple Answer Questions

**Question 6:**  
Which of the following are true about self-attention mechanisms in transformer models? (Select all that apply)

a) They compute relevance between all token pairs directly  
b) They process tokens sequentially like a linked list  
c) They enable capturing relationships regardless of distance  
d) They create a computational complexity of O(n²) with sequence length  

**Correct Answers:**  
**a) They compute relevance between all token pairs directly**  
**c) They enable capturing relationships regardless of distance**  
**d) They create a computational complexity of O(n²) with sequence length**  

**Explanation:**  
Self-attention mechanisms compute relevance directly between all token pairs, enabling relationships to be captured regardless of distance in the sequence. This creates a computational complexity of O(n²) with sequence length. They do not process tokens sequentially like a linked list, which is characteristic of RNNs.

**Question 7:**  
Which optimization techniques can significantly improve LLM performance and reduce resource requirements? (Select all that apply)

a) Quantization  
b) Pruning  
c) Distillation  
d) Increasing model parameters  

**Correct Answers:**  
**a) Quantization**  
**b) Pruning**  
**c) Distillation**  

**Explanation:**  
Quantization (reducing precision from FP16 to INT8/INT4), pruning (removing unnecessary weights), and distillation (training smaller models to mimic larger ones) are all optimization techniques that reduce resource requirements. Increasing model parameters would actually increase resource requirements, not optimize them.

**Question 8:**  
Which deployment architectures are discussed for LLM implementation? (Select all that apply)

a) Dedicated inference servers  
b) Serverless inference  
c) Edge deployment  
d) Hybrid architectures  

**Correct Answers:**  
**a) Dedicated inference servers**  
**b) Serverless inference**  
**c) Edge deployment**  
**d) Hybrid architectures**  

**Explanation:**  
All four deployment architectures are discussed in the lesson: dedicated inference servers (high-performance GPU-accelerated servers), serverless inference (on-demand scaling), edge deployment (running on client devices), and hybrid architectures (combining multiple approaches based on request complexity).

**Question 9:**  
Which advanced prompting techniques are described to enhance LLM performance? (Select all that apply)

a) Few-shot learning  
b) Chain-of-thought prompting  
c) Structured output formatting  
d) Diffusion-based generation  

**Correct Answers:**  
**a) Few-shot learning**  
**b) Chain-of-thought prompting**  
**c) Structured output formatting**  

**Explanation:**  
Few-shot learning (providing examples within the prompt), chain-of-thought prompting (requesting step-by-step reasoning), and structured output formatting (constraining response format) are all described as advanced prompting techniques. Diffusion-based generation is not mentioned in the lesson as a prompting technique.

**Question 10:**  
Which approaches are emerging for improving LLM evaluation and explainability? (Select all that apply)

a) Attention visualization  
b) Confidence scoring  
c) Contrastive explanations  
d) Randomized verification  

**Correct Answers:**  
**a) Attention visualization**  
**b) Confidence scoring**  
**c) Contrastive explanations**  

**Explanation:**  
Attention visualization (showing which parts of input influenced outputs), confidence scoring (quantifying uncertainty), and contrastive explanations (highlighting how input changes affect outputs) are all mentioned as emerging approaches for evaluation and explainability. Randomized verification is not mentioned in the lesson.