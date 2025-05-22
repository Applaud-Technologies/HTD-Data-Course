
## **Python for Data Analysis: 10-Day Learning Guide**


## **🧭 Overview**

**Audience**: Entry-level software developers who know Java, but need to learn Python for data analysis and data engineering. **Duration**: 10 days (1-2 hours per day) **Format**: Quick-reference lessons, short exercises, mini-projects, and quizzes. **Goal**: Build foundational Python skills with a focus on using libraries like NumPy and Pandas.

---

## **Day 0: Python Syntax for Java Developers**

**Focus**: Understanding Python’s syntax and how it differs from Java, enabling Java developers to adapt quickly to Python’s conventions.

Python and Java are both powerful programming languages, but their syntax differs significantly. Python emphasizes simplicity and readability, using indentation and minimal boilerplate, while Java relies on explicit declarations and curly braces. Below, we contrast key syntax elements to help you transition smoothly.

### 1. **Variable Declaration and Typing**

- **Python**: Uses dynamic typing; no need to declare variable types. Variables are assigned with a simple `=`.
    
    ```python
    name = "Alice"  # String
    age = 30        # Integer
    ```
    
- **Java**: Requires static typing with explicit type declarations.
    
    ```java
    String name = "Alice";
    int age = 30;
    ```
    
- **Key Difference**: Python infers types at runtime, reducing boilerplate but requiring care with type consistency. Java’s static typing catches type errors at compile time.

### 2. **Code Blocks and Indentation**

- **Python**: Uses indentation (typically 4 spaces) to define code blocks. No curly braces or keywords like `end`.
    
    ```python
    if age >= 18:
        print("Adult")
    else:
        print("Minor")
    ```
    
- **Java**: Uses curly braces `{}` to delimit code blocks.
    
    ```java
    if (age >= 18) {
        System.out.println("Adult");
    } else {
        System.out.println("Minor");
    }
    ```
    
- **Key Difference**: Python’s indentation is mandatory and enforces consistent formatting. Java’s braces allow flexible formatting but can lead to errors if mismatched.

### 3. **Printing to Console**

- **Python**: Uses the `print()` function, which automatically adds a newline.
    
    ```python
    print("Hello, world!")
    ```
    
- **Java**: Uses `System.out.println()` or `System.out.print()` for console output.
    
    ```java
    System.out.println("Hello, world!");
    ```
    
- **Key Difference**: Python’s `print()` is simpler and more flexible (e.g., supports f-strings). Java’s `System.out.println()` is verbose but part of its standard library.

### 4. **Semicolons**

- **Python**: Semicolons are optional and rarely used. Lines typically end with a newline.
    
    ```python
    x = 5
    y = 10
    ```
    
- **Java**: Semicolons are required to terminate statements.
    
    ```java
    int x = 5;
    int y = 10;
    ```
    
- **Key Difference**: Python’s lack of semicolons reduces clutter, but you must be mindful of line breaks. Java’s semicolons ensure statement clarity.

### 5. **Comments**

- **Python**: Single-line comments use `#`. Multi-line comments use triple quotes `"""` (technically docstrings).
    
    ```python
    # This is a single-line comment
    """This is a
       multi-line comment"""
    ```
    
- **Java**: Single-line comments use `//`, and multi-line comments use `/* */`.
    
    ```java
    // This is a single-line comment
    /* This is a
       multi-line comment */
    ```
    
- **Key Difference**: Python’s `#` is concise for single lines, and its docstrings serve dual purposes (comments and documentation). Java’s comment syntax is more explicit.

### 6. **Loops**

- **Python**: Uses `for` with `range()` or direct iteration over collections. No parentheses around conditions.
    
    ```python
    for i in range(5):
        print(i)
    ```
    
- **Java**: Uses `for` with explicit counters or enhanced for-loops. Conditions require parentheses.
    
    ```java
    for (int i = 0; i < 5; i++) {
        System.out.println(i);
    }
    ```
    
- **Key Difference**: Python’s `for` loop is more concise and iterable-focused. Java’s `for` loop is more verbose but explicit in its counter management.

### 7. **Class and Method Definitions**

- **Python**: Classes are defined with `class`, and methods use `self` explicitly. No access modifiers by default.
    
    ```python
    class Person:
        def __init__(self, name):
            self.name = name
        
        def greet(self):
            print(f"Hello, {self.name}")
    ```
    
- **Java**: Classes use `class` with explicit access modifiers (`public`, `private`). Methods use `this` implicitly.
    
    ```java
    public class Person {
        private String name;
        
        public Person(String name) {
            this.name = name;
        }
        
        public void greet() {
            System.out.println("Hello, " + name);
        }
    }
    ```
    
- **Key Difference**: Python’s class syntax is lightweight, with `self` making instance references explicit. Java’s syntax is more rigid, with mandatory access control.

### **Exercise: Syntax Conversion**

Convert the following Java code to Python, paying attention to syntax differences:

```java
public class Main {
    public static void main(String[] args) {
        int x = 10;
        if (x > 5) {
            System.out.println("x is greater than 5");
        } else {
            System.out.println("x is 5 or less");
        }
    }
}
```

**Solution**:

```python
x = 10
if x > 5:
    print("x is greater than 5")
else:
    print("x is 5 or less")
```

### **Challenge: Write a Simple Python Program**

Write a Python program that:

- Defines a variable `score` with a value of 85.
- Prints "Pass" if the score is 60 or higher, otherwise prints "Fail".
- Use Python’s syntax (no semicolons, use indentation).

**Sample Output**:

```
Pass
```

### Quiz (5 Questions)

1. How does Python indicate a code block (e.g., inside an `if` statement)?
2. What is the Python equivalent of Java’s `System.out.println()`?
3. Does Python require semicolons to end statements?
4. How do you write a single-line comment in Python?
5. What keyword does Python use instead of Java’s `else if`?

## **Day 1: Getting Started with Python**

**Focus**: Setting up your Python environment and learning the basics of the language.

**Step-by-Step Guide**:

1. **Install Python and VS Code**

   * Download Python from [python.org](https://www.python.org/downloads/).
   * Install [Visual Studio Code](https://code.visualstudio.com/).
   * In VS Code, install the Python extension by Microsoft.
   * Open a folder for your projects and create a new file: `day1.py`.
2. **Print to Console**

   * Use the `print()` function to show messages in the terminal.
   * Example:

     ```
     print("Hello, Python world!")
     ```
3. **Create and Use Variables**

   * Variables hold values like numbers or text.
   * Python uses dynamic typing (you don't declare a type).
   * Example:

     ```
     name = "Alice"
     age = 30
     is_adult = True
     ```
4. **Basic Data Types**

   * `int`: Whole numbers (`5`)
   * `float`: Decimal numbers (`3.14`)
   * `str`: Text (`"hello"`)
   * `bool`: Boolean (`True` or `False`)
   * Use `type()` to check types:

     ```
     print(type(age))  # <class 'int'>
     ```
5. **Arithmetic Operations**

   * Addition: `+`
   * Subtraction: `-`
   * Multiplication: `*`
   * Division (float): `/`
   * Division (integer): `//`
   * Remainder: `%`
   * Power: `**`
   * Example:

     ```
     x = 5
     y = 2
     print(x / y)   # 2.5
     print(x // y)  # 2
     print(x % y)   # 1
     print(x ** y)  # 25
     ```
6. **Using \*\***\`\`**\*\*\*\* to Read User Input\*\***

   * Use `input()` to get text input from the user.
   * You can convert input to a number using `int()` or `float()`.
   * Example:

     ```
     age = input("Enter your age: ")
     print("You are", age, "years old")
     ```

     Use `int(age)` if you need it as a number.

---

**Code Sample Recap**:

```
# Hello World
print("Hello, Python world!")
​
# Variables and arithmetic
x = 10
y = 5
print("Sum:", x + y)
print("Division:", x / y)
```

---

**Exercise**: Write a script that asks the user to input three numbers. Then calculate and print:

* Their total
* Their average
* Their product

**Example Output**:

```
Enter three numbers: 2 4 6
Total: 12
Average: 4.0
Product: 48
```

**Bonus Challenge**: Refactor your script to reject non-numeric input using `try/except`:

```
try:
    nums = input("Enter 3 numbers separated by spaces: ").split()
    a, b, c = map(float, nums)
    print("Total:", a + b + c)
    print("Average:", (a + b + c) / 3)
    print("Product:", a * b * c)
except ValueError:
    print("Please enter valid numbers.")
```

---

**Quiz (5 Questions)**:

1. What function do you use to display output in Python?
2. What data type is the result of `5 / 2`?
3. What keyword is used to define a variable in Python?
4. What is the output of `print(2 ** 3)`?
5. Which function returns the type of a variable?


---


## **Day 2: Working with Strings & Input**

Working with text in Python is essential, especially in data-related tasks. On Day 2, we focus on how to handle strings and user input effectively.

Strings in Python are defined by wrapping characters in either single (`'`) or double (`"`) quotes. You can assign them to variables just like numbers:

```
name = "Alice"
message = 'Welcome to Python!'
```

Python provides many built-in methods for string manipulation. For instance, you can make a string uppercase using `.upper()`, or replace parts of it using `.replace()`:

```
print(name.upper())            # Output: ALICE
print(message.replace("Python", "Programming"))
```

You can also use formatted strings (f-strings) to embed variables directly within strings. This is useful for readable output:

```
age = 30
print(f"{name} is {age} years old.")
```

To extract parts of a string, Python supports slicing using bracket notation with indices. For example:

```
greeting = "Hello, world!"
print(greeting[0:5])  # Output: Hello
print(greeting[-6:])  # Output: world!
```

This technique is known as substring slicing. The first number is the start index (inclusive), and the second is the end index (exclusive).

The `input()` function allows you to get information from users as a string. You can then convert that input into other types like integers or floats using type conversion functions:

```
number = input("Enter a number: ")
number = int(number)  # Convert the input to an integer
```

It’s important to remember that input is always read as a string, so explicit conversion is necessary if you plan to perform mathematical operations.

Here are some of the most common string methods in Python:

| **MethodDescriptionExample Output** |                                     |                                          |
| ----------------------------------- | ----------------------------------- | ---------------------------------------- |
| `.upper()`                          | Converts to uppercase               | `'hello'.upper()` → `'HELLO'`            |
| `.lower()`                          | Converts to lowercase               | `'HELLO'.lower()` → `'hello'`            |
| `.strip()`                          | Removes leading/trailing whitespace | `' hello '.strip()` → `'hello'`          |
| `.replace()`                        | Replaces substring                  | `'a,b,c'.replace(',', ';')` → `'a;b;c'`  |
| `.find()`                           | Returns index of substring or -1    | `'hello'.find('e')` → `1`                |
| `.startswith()`                     | Checks if string starts with prefix | `'data.csv'.startswith('data')` → `True` |
| `.split()`                          | Splits string into list             | `'a,b,c'.split(',')` → `['a','b','c']`   |
| `.join()`                           | Joins list into string              | `','.join(['a','b','c'])` → `'a,b,c'`    |

---

**Exercise**: Build a simple calculator Prompt the user to enter two numbers and an operation (add, subtract, multiply, or divide). Use `input()` and string handling to determine which operation to apply and output the result.

**Example Interaction**:

```
Enter first number: 10
Enter operation (+, -, *, /): *
Enter second number: 5
Result: 50
```

**Challenge**: Format a user report Ask the user for their first name, last name, and favorite number. Then, output a well-formatted multi-line report using an f-string.

**Sample Output**:

```
User Report
===========
Name: Alice Smith
Favorite Number: 42
```


---


## **Day 3: Lists and Tuples**

Lists and tuples are two of Python’s core data structures for storing ordered collections of items. Lists are mutable (changeable), while tuples are immutable (read-only after creation).

Let’s begin with lists. Lists are created using square brackets:

```
fruits = ["apple", "banana", "cherry"]
```

You can access individual items using zero-based indexing, which means that the first item in the list is at position 0, the second item is at position 1, and so on. For example, in a list `fruits = ["apple", "banana", "cherry"]`, `fruits[0]` would return "apple".

Python also allows negative indexing, which starts counting from the end of the list. So `fruits[-1]` gives you the last item in the list ("cherry" in this case), `fruits[-2]` gives the second-to-last item, and so forth.

```
print(fruits[0])  # apple
print(fruits[-1])  # cherry
```

Lists can be sliced to retrieve subsets of items using a technique called slicing. This allows you to extract a portion of the list by specifying a start index and an end index using the syntax `list[start:end]`. The start index is inclusive, meaning it starts from that position, while the end index is exclusive, meaning it stops right before that index.

For example, in the list `fruits = ["apple", "banana", "cherry", "date"]`, the slice `fruits[0:2]` will return `['apple', 'banana']` because it includes index 0 and 1, but not 2.

You can also omit one of the indices:

* `fruits[:2]` starts at the beginning and stops at index 2 (exclusive).
* `fruits[2:]` starts at index 2 and goes to the end of the list.

Python also supports negative indices in slicing. For example, `fruits[-3:-1]` returns `['banana', 'cherry']`.

**Watch out for:**

* If the start index is greater than or equal to the end index, you'll get an empty list.
* If your indices are out of range, Python will not raise an error—it simply returns what it can.
* Slicing does not modify the original list; it returns a new one.

Slicing is a very powerful feature in Python and becomes especially useful in data analysis when you're working with segments of datasets.

Python also supports common list operations like adding, removing, and updating elements:

```
fruits.append("date")  # Add to end
fruits.insert(1, "blueberry")  # Insert at index
fruits.remove("banana")  # Remove by value
fruits[0] = "apricot"  # Update value at index 0
```

You can loop through a list with a `for` loop:

```
for fruit in fruits:
    print(fruit)
```

Tuples are similar to lists in that they store an ordered collection of items, but they differ in one key way: tuples are immutable. This means that once a tuple is created, its contents cannot be changed. In Python, tuples are defined using parentheses instead of square brackets:

```
colors = ("red", "green", "blue")
```

For Java developers, think of a tuple as being similar to an array that cannot be modified after creation—like a final array—but with some added flexibility. Tuples can hold mixed data types and are often used when you want to return multiple values from a function, or when you need to group related values without the overhead of creating a class or struct.

Because tuples are immutable, they offer some performance benefits and can be used as dictionary keys or placed in sets (which require immutable elements). However, if you need to modify the values, you’ll have to convert the tuple into a list:

```
colors_list = list(colors)
colors_list.append("yellow")
```

This immutability makes tuples a great choice for fixed configurations, coordinate pairs, or any data that shouldn't change during execution.

Tuples are useful for representing fixed collections of data, such as coordinates:

```
point = (3, 4)
```

Although you can’t modify tuples, you can convert them to lists if needed:

---

**Exercise**: Grocery List Manager Create a program that allows the user to add items to a grocery list, remove items, and display the list. Include options like:

```

- `add item`
- `remove item`
- `view list`
- `exit`
  Use a `while` loop to keep prompting the user until they choose to exit.
​
**Sample Interaction**:
​
​
​
> add milk
> add eggs
> remove milk
> view
> \['eggs']
> exit
​
```

**Mini Project**: Budget Planner List Build a simple tool that tracks a list of expenses. Each entry should be a tuple with a description and amount (e.g., `("Groceries", 50.0)`). After the user is done entering expenses, print out:

* A formatted list of all expenses
* The total amount spent

**Sample Output**:

```

Expenses:
​
* Groceries: \$50.00
* Rent: \$1000.00
  Total Spent: \$1050.00
​
```


---


## **Day 4: Dictionaries and Sets**

Dictionaries and sets are powerful data structures in Python, often used for efficient data lookup and elimination of duplicates, respectively.

A **dictionary** stores data in key-value pairs. Unlike lists which use integer indices, dictionaries use keys (which must be immutable) to access values.

Here’s how to create and use a dictionary:

```
person = {
    "name": "Alice",
    "age": 30,
    "city": "New York"
}
​
print(person["name"])  # Output: Alice
```

You can add or update values like this:

```
person["email"] = "alice@example.com"  # Add new key
person["age"] = 31                     # Update value
```

To avoid a `KeyError` when accessing keys, use the `.get()` method:

```
print(person.get("phone", "Not provided"))
```

Looping through dictionaries:

```
for key, value in person.items():
    print(f"{key}: {value}")
```

A **set** is an unordered collection of unique values. Sets are useful when you need to eliminate duplicates or perform fast membership tests:

```
fruits = {"apple", "banana", "cherry"}
fruits.add("apple")  # Will not add a duplicate
print("banana" in fruits)  # True
```

You can also perform set operations like union, intersection, and difference:

```
set1 = {1, 2, 3}
set2 = {3, 4, 5}
print(set1 | set2)  # Union: {1, 2, 3, 4, 5}
print(set1 & set2)  # Intersection: {3}
print(set1 - set2)  # Difference: {1, 2}
```

---

**Exercise**: Frequency Counter from Text Write a program that asks the user to input a sentence. Then, use a dictionary to count how many times each word appears. Convert the sentence to lowercase and split it into words using `.split()`.

**Example Input**:

```
The quick brown fox jumps over the lazy dog. The dog was not amused.
```

**Example Output**:

```
the: 3
quick: 1
brown: 1
...
```

---

**Quiz**:

1. What is the correct syntax to access a value in a dictionary by key?
2. What is the output of `{1, 2, 2, 3}`?
3. Which method is used to loop over both keys and values in a dictionary?
4. How can you safely access a key in a dictionary without risking an error?
5. What is the result of `{1, 2, 3} & {2, 3, 4}`?


---


## **Day 5: Control Flow**

Control flow refers to how a program decides which lines of code to execute based on conditions and repetition. Python uses plain English keywords (`if`, `elif`, `else`, `for`, `while`) that make control structures readable and intuitive.

##### **Conditional Statements**

Python uses indentation (typically 4 spaces) to define code blocks, unlike Java’s braces `{}`.

```
age = 20
if age < 18:
    print("Minor")
elif age < 65:
    print("Adult")
else:
    print("Senior")

```

**Note**: `elif` means “else if” in Python, and `else` is optional.

##### **Comparison and Logical Operators**

Python uses these operators for comparisons:

| Operation Symbol |      |
| ---------------- | ---- |
| Equal            | `==` |
| Not equal        | `!=` |
| Greater than     | `>`  |
| Less than        | `<`  |
| Greater or equal | `>=` |
| Less or equal    | `<=` |

Logical combinations:

* `and`, `or`, and `not`

```
score = 90
if score > 80 and score < 100:
    print("Great job!")

```

---

##### \*\*Loops: \*\*`** and **`

Python `for` loops are usually used to iterate over a sequence:

```
for i in range(5):
    print(i)
# Outputs 0 through 4

```

* `range(n)` generates numbers from 0 to `n - 1`
* You can also use `range(start, stop, step)`

```
for i in range(2, 10, 2):
    print(i)
# Outputs: 2, 4, 6, 8

```

A `while` loop continues while a condition is `True`:

```
count = 0
while count < 3:
    print("Count is", count)
    count += 1

```

---

##### **Control Keywords**

* `break` — exits a loop early
* `continue` — skips to the next iteration

```
for i in range(5):
    if i == 3:
        break
    print(i)

```

---

##### **Exercise: FizzBuzz**

Write a loop from 1 to 100. For each number:

* Print `Fizz` if divisible by 3
* Print `Buzz` if divisible by 5
* Print `FizzBuzz` if divisible by both
* Otherwise, print the number

```
for i in range(1, 101):
    if i % 3 == 0 and i % 5 == 0:
        print("FizzBuzz")
    elif i % 3 == 0:
        print("Fizz")
    elif i % 5 == 0:
        print("Buzz")
    else:
        print(i)

```

---

##### **Challenge: Detect Palindromes in a List**

Create a list of strings and print only the palindromes (words that read the same backward):

```
words = ["radar", "apple", "level", "banana"]
for word in words:
    if word == word[::-1]:
        print(f"{word} is a palindrome")

```

The `[::-1]` syntax is a Python slice trick that reverses a string.

---


---


## **Day 6: Functions**

Functions are a fundamental concept in Python that help organize your code into reusable blocks. They allow you to break down complex problems into manageable parts and avoid writing repetitive code.

You define a function using the `def` keyword, followed by the function name and parentheses. Here's a simple example:

```python
def greet():
    print("Hello!")

# Calling the function
greet()
```

Functions can accept parameters (also called arguments) so they can perform actions using the data you provide. They can also return values using the `return` keyword:

```python
def add(a, b):
    return a + b

result = add(3, 4)
print(result)  # 7
```

In Python, function parameters can have default values, which are used if the caller does not supply that argument:

```python
def greet(name="friend"):
    print(f"Hello, {name}!")

greet("Alice")       # Hello, Alice!
greet()              # Hello, friend!
```

Functions also have **scope**, which means variables defined inside a function are only accessible inside that function. This helps avoid conflicts with other variables in your code:

```python
def show():
    message = "I'm local to this function"
    print(message)

show()
# print(message)  # This would raise an error
```

---

**Exercise: Temperature Converter**
Write a function `to_celsius(fahrenheit)` that converts a temperature in Fahrenheit to Celsius. Then ask the user for a temperature in Fahrenheit, convert it using your function, and print the result.

**Formula**: `(F - 32) * 5 / 9`

---

**Mini Project: Modular BMI Calculator**
Build a small BMI calculator using two functions:

1. `calculate_bmi(weight_kg, height_m)` – computes BMI
2. `classify_bmi(bmi)` – returns a string classification like "Underweight", "Normal", or "Overweight"

Ask the user for weight and height, call your functions, and print the result nicely.

**Bonus**: Use default values to allow testing without input.


---


## **Day 7: Working with Files and Exceptions**

Reading and writing files is a critical skill for any data engineer or analyst. Python makes this task simple with built-in support for file I/O using the `open()` function. Today, we’ll also explore how to handle runtime errors gracefully using exceptions.

Let’s begin by reading from a file. Here’s a common pattern using a context manager to safely open and read a file:

```python
with open("data.txt", "r") as file:
    contents = file.read()
    print(contents)
```

The `with` keyword ensures the file is closed properly, even if an error occurs. The mode "r" means read-only. If the file is large, you can iterate through it line by line:

```python
with open("data.txt", "r") as file:
    for line in file:
        print(line.strip())
```

To write to a file, use mode "w" (overwrite) or "a" (append):

```python
with open("output.txt", "w") as file:
    file.write("This is a test message.
")
```

If something goes wrong, like trying to open a missing file, Python will raise an error (called an exception). You can handle exceptions using `try/except`:

```python
try:
    with open("missing.txt", "r") as file:
        print(file.read())
except FileNotFoundError:
    print("That file doesn't exist.")
```

This approach prevents your script from crashing and lets you provide helpful feedback to users.

---

**Bonus: Working with CSV Files**

CSV (Comma-Separated Values) is one of the most widely used formats for storing tabular data, especially in spreadsheets and data exports. A CSV file contains plain text where each line represents a row of data, and each value within that row is separated by a comma.

Python provides a built-in `csv` module that makes it straightforward to work with these files. Let’s first look at how to read a CSV file:

```python
import csv

with open("people.csv", "r") as file:
    reader = csv.reader(file)
    for row in reader:
        print(row)
```

This code opens a file named `people.csv`, reads it line by line, and prints each row as a list. Every item in that list represents a single cell from the row. If your file includes a header (column names), the first row will contain those headers.

If you need more structured access, such as referring to columns by name, you can use `csv.DictReader`:

```python
with open("people.csv", "r") as file:
    reader = csv.DictReader(file)
    for row in reader:
        print(row["Name"], row["Age"])
```

To write CSV files, you use `csv.writer`, and for dictionary-based writing, `csv.DictWriter`. Always use `newline=""` on Windows to avoid blank lines between rows:

```python
with open("output.csv", "w", newline="") as file:
    writer = csv.writer(file)
    writer.writerow(["Name", "Age"])
    writer.writerow(["Alice", 30])
```

This will create a file with the following contents:

```
Name,Age
Alice,30
```

Working with CSV files is an essential skill when processing datasets, logs, exports from databases, or even Google Sheets data.

```python
import csv

with open("people.csv", "r") as file:
    reader = csv.reader(file)
    for row in reader:
        print(row)
```

To write CSV:

```python
with open("output.csv", "w", newline="") as file:
    writer = csv.writer(file)
    writer.writerow(["Name", "Age"])
    writer.writerow(["Alice", 30])
```

---

**Exercise**: Word Counter
Create a file called `sample.txt` with a few sentences. Then write a program that opens the file, counts the frequency of each word, and prints the results. Be sure to:

* Convert text to lowercase
* Strip punctuation (optional for now)
* Use a dictionary to count occurrences

---

**Quiz**:

1. What does the `with open(...) as file:` pattern help prevent?
2. Which mode should you use to add content to the end of a file without overwriting?
3. What exception is raised when you try to open a non-existent file?
4. What Python module helps with reading CSV files?
5. What happens if you forget to close a file after writing to it?


---


## **Day 8: Introduction to NumPy**

As you begin working with larger datasets and performing mathematical operations, Python lists start to show their limitations. That’s where NumPy (short for Numerical Python) comes in. It’s a powerful library designed to handle large, multi-dimensional arrays and perform high-performance numerical computations.

Before using NumPy, you need to install it. If you're using pip:

```bash
pip install numpy
```

Once installed, you can import it using the conventional alias:

```python
import numpy as np
```

##### Arrays vs Lists

NumPy’s primary object is the `ndarray` (N-dimensional array). Think of it like a Python list, but more powerful and efficient. Here's a basic comparison:

```python
import numpy as np

# Python list
lst = [1, 2, 3, 4]

# NumPy array
arr = np.array([1, 2, 3, 4])
```

NumPy arrays are more memory-efficient and support element-wise operations directly:

```python
print(arr * 2)  # Output: [2 4 6 8]
```

Try doing that with a list and you’ll get `[1, 2, 3, 4, 1, 2, 3, 4]` — a repeated list, not a math operation.

##### Creating Arrays

NumPy provides a variety of ways to create arrays:

```python
np.zeros(5)       # [0. 0. 0. 0. 0.]
np.ones(3)        # [1. 1. 1.]
np.arange(0, 10)  # [0 1 2 3 4 5 6 7 8 9]
np.linspace(0, 1, 5)  # [0.   0.25 0.5  0.75 1.  ]
```

##### Array Operations

NumPy makes it easy to apply mathematical operations across all elements:

```python
arr = np.array([1, 2, 3])
print(arr + 1)       # [2 3 4]
print(arr ** 2)      # [1 4 9]
print(np.sqrt(arr))  # [1. 1.4142 1.732]
```

You can also perform useful statistical functions:

```python
print(arr.mean())  # Average
print(arr.sum())   # Total
print(arr.max())   # Maximum
```

**Manipulating Arrays**

NumPy arrays aren't just powerful because they're fast—they're also flexible. One of the key features that sets them apart from lists is their ability to be reshaped and analyzed using axis-based operations.

The `reshape()` function allows you to change the shape of your array without changing its data. For example, if you have a one-dimensional array of 20 elements, you can turn it into a 4-row by 5-column (4x5) two-dimensional array:

```
arr = np.arange(1, 21)        # [1, 2, ..., 20]
reshaped = arr.reshape(4, 5)  # Now it's a 4x5 matrix
print(reshaped)

```

Understanding the **axis** parameter is important when performing operations like summing or averaging data. In NumPy:

* `axis=0` means "down the rows" (i.e., perform the operation on each column)
* `axis=1` means "across the columns" (i.e., perform the operation on each row)

Example:

```
print(reshaped.sum(axis=0))  # Sum of each column
print(reshaped.sum(axis=1))  # Sum of each row

```

You can use `.mean()` the same way:

```
print(reshaped.mean())       # Mean of all values
print(reshaped.mean(axis=0)) # Mean of each column

```

This axis-based behavior is incredibly useful in data analysis, where rows typically represent records and columns represent features. Understanding how to control these operations is essential to getting insights from your data.

**Exercise**: Create and Analyze a NumPy Array. Create an array with values from 1 to 20. Then:

* Reshape it into a 4x5 matrix
* Print the sum of each row and column
* Find the overall mean

**Hint**: Use `reshape()`, `axis=0`, and `axis=1` with `sum()`

---

**Challenge**: Simulate 100 Dice Rolls
Use NumPy to simulate 100 dice rolls (numbers from 1 to 6), then count how many times each number appears. Use `np.random.randint()` and `np.unique(..., return_counts=True)`

---


---


## **Day 9: Introduction to Pandas**

As a Java developer, you might be used to dealing with structured data through arrays, lists, or even `ResultSet` objects from JDBC. In Python, the go-to tool for structured, tabular data is **Pandas**. It's essentially the `Excel + SQL + Java Collections` of Python.

Pandas is built on top of NumPy and provides two main data structures:

* **Series**: A 1D array (like a single column in a spreadsheet)
* **DataFrame**: A 2D table with rows and columns (like a SQL table or Excel sheet)

You start by importing Pandas:

```python
import pandas as pd
```

##### Reading CSV Files

The most common way to load data is with `pd.read_csv()`:

```python
df = pd.read_csv("sales.csv")
print(df.head())  # Shows the first 5 rows
```

Gotcha: Make sure your file path is correct. If the CSV is in the same folder, `"sales.csv"` works. If not, provide the full path. Use raw strings (`r"C:\path\to\file.csv"`) on Windows to avoid escape issues.

##### Indexing and Filtering

Want to select a column? Just use the column name like a dictionary key:

```python
print(df["Revenue"])
```

You can also filter rows with conditional logic:

```python
high_sales = df[df["Revenue"] > 10000]
print(high_sales)
```

For Java devs: this is like filtering a stream of objects using `filter()`.

##### Aggregation and Basic Stats

Pandas makes it very easy to get basic stats on your dataset:

```python
print(df.describe())  # Summary stats (mean, std, min, max...)
print(df["Revenue"].sum())  # Total revenue
print(df["Revenue"].mean())  # Average revenue
```

You can also group by a column and aggregate:

```python
print(df.groupby("Region")["Revenue"].sum())
```

This is similar to a `GROUP BY` clause in SQL.

---

**Mini Project**: Analyze a Sample Sales Dataset

1. Load `sales.csv` using `read_csv()`
2. Print the first few rows and column names
3. Filter the dataset to include only rows with revenue over \$10,000
4. Calculate the total and average revenue
5. Group revenue by region and sort it in descending order

Gotcha: Watch out for case sensitivity and whitespace in column names—use `df.columns` to inspect them if your filtering doesn’t work as expected.

If the data has missing values, Pandas will represent them as `NaN`. You can handle them with:

```python
df.dropna()        # Removes rows with missing values
df.fillna(0)       # Fills missing values with 0
```

---


---


## **Day 10: Bringing it Together + Classes**

Since you're coming from a Java background, you're already familiar with Object-Oriented Programming concepts like classes, methods, and encapsulation. Python supports OOP too, but the syntax is more lightweight and flexible.

##### Refresher: How Classes Work in Python

In Python, classes are defined with the `class` keyword, and methods must include `self` as their first parameter. This is roughly equivalent to using `this` in Java:

```python
class Person:
    def __init__(self, name, age):
        self.name = name
        self.age = age

    def greet(self):
        print(f"Hi, I'm {self.name} and I'm {self.age} years old.")

p = Person("Alice", 30)
p.greet()
```

* Python doesn't require access modifiers (`public`, `private`) but you can use `_` and `__` conventions to indicate internal use.
* The `__init__` method is like a constructor in Java.

##### Using Classes with Pandas

Let’s say you're analyzing sales data across multiple reports. Instead of duplicating your analysis logic, you can wrap it in a reusable class.

Here’s a simple example of a class that wraps a Pandas DataFrame:

```python
import pandas as pd

class SalesReport:
    def __init__(self, csv_path):
        self.df = pd.read_csv(csv_path)

    def high_revenue(self, threshold):
        return self.df[self.df["Revenue"] > threshold]

    def total_by_region(self):
        return self.df.groupby("Region")["Revenue"].sum().sort_values(ascending=False)
```

This lets you instantiate `SalesReport` objects for different files and call methods to analyze them consistently:

```python
report = SalesReport("sales.csv")
print(report.high_revenue(10000))
print(report.total_by_region())
```

##### Final Project: Build a Data Report Generator

Create a `DataAnalyzer` class with the following features:

* Constructor takes a CSV file path and loads it into a DataFrame
* A method to describe the dataset using `df.describe()`
* A method to drop missing values
* A method to compute summary stats for a given column
* Bonus: Add logging and error handling

**Sample Usage**:

```python
analyzer = DataAnalyzer("sales.csv")
analyzer.describe()
analyzer.drop_missing()
print(analyzer.column_summary("Revenue"))
```

##### Final Quiz (10 Questions Across All Topics)

1. What’s the difference between a list and a tuple?
2. What does the `axis=1` parameter do in NumPy or Pandas?
3. What’s the default data type returned from `input()`?
4. How do you open a file for appending text?
5. How do you handle missing values in Pandas?
6. What does `self` represent in a Python method?
7. Which keyword defines a function?
8. How do you select a subset of rows in a DataFrame?
9. What function would you use to read a CSV?
10. How do you simulate random numbers in NumPy?

