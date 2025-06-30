# Lab: Crafting a Prompt Template for Decoding Python Errors

**Instructions:** For each scenario, craft a Chain-of-Thought (CoT) prompt to debug a Python error, following lesson patterns. Ensure clear constraints, structured tasks, and formatted output.

---

## Scenario 1: Debugging a `NameError` in a Transaction Processing Script

You need to help a data engineer debug a Python script that processes a list of transactions but fails with a cryptic `NameError` due to variable shadowing in a list comprehension.  

**Key Details**:
- The script filters transactions above $100 and returns a list of valid amounts.
- The error occurs when trying to print a variable used in a list comprehension.
- The engineer is using VS Code with the Python extension and Pylint linter.
- The error must be explained in beginner-friendly terms, and solutions must use only VS Code’s built-in tools.
- The fix should take no more than 10 minutes to implement.

**Error Message**:
```
NameError: name 'amt' is not defined
File "script.py", line 3
```

**Code Snippet**:
```python
def filter_transactions(transactions):
    result = [amt for amt in transactions if amt > 100]
    print(amt)  # 'amt' is from the list comprehension
    return result

transactions = [50, 150, 75, 200]
filter_transactions(transactions)
```

**Student Task**: Write a chain-of-thought prompt that guides the AI to systematically parse the `NameError`, explain its cause (focusing on variable scoping in list comprehensions), propose 2-3 actionable solutions using VS Code, and suggest a preventive measure to avoid similar errors. Ensure the prompt includes a role for the AI, constraints (e.g., beginner-friendly, VS Code-only tools), and a clear output format with sections for error analysis, cause explanation, solutions, and prevention tips.

---

## Scenario 2: Resolving an `IndentationError` in a Data Validation Function

You need to assist a programmer in fixing a Python function that validates data but fails with an `IndentationError` due to inconsistent indentation after copying code from a text editor.  
**Key Details**:

- The function checks if input data is None and returns a boolean.
- The error is caused by mixing tabs and spaces in the code.
- The graduate uses VS Code with no linter installed.
- Solutions must be implementable without installing additional extensions.
- The explanation must clarify why the error message is confusing for beginners.

**Error Message**:

```
IndentationError: unindent does not match any outer indentation level
File "script.py", line 4
```

**Code Snippet**:

```python
def validate_data(data):
    if data is None:
        return False
     return True  # Mixed tabs and spaces
```

**Student Task**: Write a chain-of-thought prompt that guides the AI to analyze the `IndentationError`, explain the issue (emphasizing tabs vs. spaces), provide 2-3 fixes using only VS Code’s built-in features, and recommend a preventive configuration. The prompt must assign a role to the AI, include constraints (e.g., no external tools, beginner-friendly), and specify an output format with error analysis, cause explanation, solutions, and prevention tips.

---

## Scenario 3: Fixing a `TypeError` in an Order Value Calculation

You need to support a data engineer debugging a script that calculates the average order value but fails with a `TypeError` due to incorrect string concatenation with an integer.  

**Key Details**:
- The script sums order values and computes their average.
- The error occurs when trying to concatenate a string with an integer in a loop.
- The engineer uses VS Code with the Python extension and Flake8 linter.
- Solutions must maintain the script’s functionality (calculate average) and be clear for beginners.
- The fix must be implementable in under 15 minutes.

**Error Message**:
```
TypeError: can only concatenate str (not "int") to str
File "script.py", line 4
```

**Code Snippet**:
```python
def calculate_avg_order(orders):
    total = 0
    for order in orders:
        total += "Order: " + order  # order is an integer
    return total / len(orders)

orders = [100, 200, 300]
calculate_avg_order(orders)
```

**Student Task**: Write a chain-of-thought prompt that guides the AI to dissect the `TypeError`, explain the string-integer mismatch, offer 2-3 solutions using VS Code and Flake8, and suggest a preventive practice. The prompt must define an AI role, include constraints (e.g., beginner-friendly, maintain functionality), and provide an output format with error analysis, cause explanation, solutions, and prevention tips.

---

## Reflection:

1. Can any of the prompts you created above serve as a single prompt template you could use a permanent tool in your debugging toolkit?


2. How many categories of AI-assisted translation of error messages can you create to assist your daily work as data engineer?