# BookHaven Data Warehouse Data Dictionary

## dim_book
| Column       | Type         | Description                        | Notes                |
|--------------|--------------|------------------------------------|----------------------|
| book_key     | INT (PK)     | Surrogate key for book             | Identity, PK         |
| isbn         | VARCHAR(20)  | Book ISBN                          | Business key         |
| title        | VARCHAR(255) | Book title                         |                      |
| genre        | VARCHAR(50)  | Book genre                         |                      |
| series       | VARCHAR(100) | Book series name                   | Nullable             |
| recommended  | VARCHAR(10)  | Recommendation flag                | 'Yes', 'No', ''      |

- **SCD Type 1**: Overwrite changes to book attributes (e.g., title, genre).

## dim_author
| Column      | Type         | Description                        | Notes                |
|-------------|--------------|------------------------------------|----------------------|
| author_key  | INT (PK)     | Surrogate key for author           | Identity, PK         |
| name        | VARCHAR(100) | Author name                        |                      |
| email       | VARCHAR(100) | Author email                       |                      |
| phone       | VARCHAR(30)  | Author phone number                |                      |
| genres      | VARCHAR(255) | Genres written by author           | CSV or JSON string   |

- **SCD Type 1**: Overwrite changes to author attributes.

## dim_customer
| Column           | Type         | Description                        | Notes                |
|------------------|--------------|------------------------------------|----------------------|
| customer_key     | INT (PK)     | Surrogate key for customer         | Identity, PK         |
| name             | VARCHAR(100) | Customer name                      |                      |
| email            | VARCHAR(100) | Customer email                     |                      |
| phone            | VARCHAR(30)  | Customer phone number              |                      |
| genre_preferences| VARCHAR(255) | Customer genre preferences         | CSV or JSON string   |

- **SCD Type 1**: Overwrite changes to customer attributes.

## dim_date
| Column    | Type      | Description                | Notes                |
|-----------|-----------|----------------------------|----------------------|
| date_key  | INT (PK)  | Surrogate key for date     | YYYYMMDD format      |
| date      | DATE      | Calendar date              |                      |
| year      | INT       | Year                       |                      |
| month     | INT       | Month                      |                      |
| day       | INT       | Day                        |                      |

## fact_book_sales
| Column       | Type           | Description                        | Notes                |
|--------------|----------------|------------------------------------|----------------------|
| sales_key    | INT (PK)       | Surrogate key for sale             | Identity, PK         |
| book_key     | INT (FK)       | Linked book                        | FK to dim_book       |
| author_key   | INT (FK)       | Linked author                      | FK to dim_author     |
| customer_key | INT (FK)       | Linked customer                    | FK to dim_customer   |
| date_key     | INT (FK)       | Linked date                        | FK to dim_date       |
| quantity     | INT            | Number of books sold               |                      |
| price        | DECIMAL(10,2)  | Sale price per book                |                      |

---

**Notes:**
- All surrogate keys are auto-incrementing primary keys.
- All FKs enforce referential integrity.
- SCD Type 1: Only the latest attribute values are kept in dimension tables. 