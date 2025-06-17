-- BookHaven Data Warehouse Star Schema

-- Dimension: Book
CREATE TABLE dim_book (
    book_key INT IDENTITY(1,1) PRIMARY KEY,
    isbn VARCHAR(20) NOT NULL,
    title VARCHAR(255),
    genre VARCHAR(50),
    series VARCHAR(100),
    recommended VARCHAR(10)
);

-- Dimension: Author
CREATE TABLE dim_author (
    author_key INT IDENTITY(1,1) PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    phone VARCHAR(30),
    genres VARCHAR(255)
);

-- Dimension: Customer
CREATE TABLE dim_customer (
    customer_key INT IDENTITY(1,1) PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    phone VARCHAR(30),
    genre_preferences VARCHAR(255)
);

-- Dimension: Date
CREATE TABLE dim_date (
    date_key INT PRIMARY KEY,
    date DATE,
    year INT,
    month INT,
    day INT
);

-- Fact: Book Sales
CREATE TABLE fact_book_sales (
    sales_key INT IDENTITY(1,1) PRIMARY KEY,
    book_key INT,
    author_key INT,
    customer_key INT,
    date_key INT,
    quantity INT,
    price DECIMAL(10,2),
    FOREIGN KEY (book_key) REFERENCES dim_book(book_key),
    FOREIGN KEY (author_key) REFERENCES dim_author(author_key),
    FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key),
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key)
); 