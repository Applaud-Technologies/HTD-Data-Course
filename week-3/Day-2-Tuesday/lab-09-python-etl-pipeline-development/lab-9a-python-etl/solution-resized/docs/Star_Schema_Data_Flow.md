# Star Schema ETL Pipeline Data Flow

This document provides a comprehensive Mermaid diagram showing the complete data flow through the Star Schema ETL solution components.

## Complete Star Schema ETL Data Flow

```mermaid
graph TD
    %% Data Sources
    A[customers.csv<br/>Customer Data] --> E[extract.py<br/>DataExtractor]
    B[products.json<br/>Product Data] --> E
    C[SQL Server<br/>sales_transactions<br/>Baseline Data] --> E
    
    %% Configuration Management
    D[config.py<br/>StarSchemaConfig<br/>- Database Settings<br/>- Table Definitions<br/>- Processing Config] --> E
    D --> F[transform.py<br/>StarSchemaTransformer]
    D --> G[load_dimensions.py<br/>DimensionLoader]
    D --> H[load_facts.py<br/>FactLoader]
    D --> I[setup_star_schema.py<br/>StarSchemaBuilder]
    
    %% Database Setup Phase
    I --> J[(Star Schema Database<br/>- dim_customer<br/>- dim_product<br/>- dim_date<br/>- dim_sales_rep<br/>- fact_sales)]
    
    %% Extraction Phase
    E --> K[Raw Extracted Data<br/>- customers: List[Dict]<br/>- products: List[Dict]<br/>- transactions: List[Dict]<br/>- sales_reps: List[Dict]]
    
    %% Transformation Phase
    K --> F
    F --> L[Transformed Data<br/>- dim_customer: Clean records<br/>- dim_product: Enriched records<br/>- dim_sales_rep: Standardized records<br/>- fact_sales: Calculated measures<br/>- date_keys: YYYYMMDD format]
    
    %% Dimension Loading Phase
    L --> G
    G --> J
    G --> M[Dimension Key Mappings<br/>- customer_keys: Dict[str, int]<br/>- product_keys: Dict[str, int]<br/>- sales_rep_keys: Dict[str, int]<br/>- date_keys: List[int]]
    
    %% Fact Loading Phase
    L --> H
    M --> H
    H --> J
    
    %% Pipeline Orchestration
    N[main.py<br/>StarSchemaETLPipeline<br/>- Phase Coordination<br/>- Error Handling<br/>- Progress Tracking] --> O[Phase 1: Database Setup]
    N --> P[Phase 2: Data Extraction]
    N --> Q[Phase 3: Data Transformation]
    N --> R[Phase 4: Dimension Loading]
    N --> S[Phase 5: Fact Loading]
    N --> T[Phase 6: Validation]
    
    %% Phase Execution Flow
    O --> I
    P --> E
    Q --> F
    R --> G
    S --> H
    T --> U[Data Validation<br/>- Row Count Checks<br/>- FK Integrity<br/>- Business Rules<br/>- Quality Metrics]
    
    %% Star Schema Structure Detail
    J --> V[dim_customer<br/>customer_key PK<br/>customer_id UK<br/>+ attributes]
    J --> W[dim_product<br/>product_key PK<br/>product_id UK<br/>+ attributes]
    J --> X[dim_date<br/>date_key PK<br/>calendar attributes<br/>pre-populated]
    J --> Y[dim_sales_rep<br/>sales_rep_key PK<br/>sales_rep_id UK<br/>+ attributes]
    J --> Z[fact_sales<br/>sales_key PK<br/>customer_key FK<br/>product_key FK<br/>date_key FK<br/>sales_rep_key FK<br/>+ measures]
    
    %% Foreign Key Relationships
    V -.->|customer_key| Z
    W -.->|product_key| Z
    X -.->|date_key| Z
    Y -.->|sales_rep_key| Z
    
    %% Analytics and Reporting
    Z --> AA[Analytics Queries<br/>- Sales by Customer Segment<br/>- Product Performance<br/>- Time-based Analysis<br/>- Cross-dimensional Reports]
    V --> AA
    W --> AA
    X --> AA
    Y --> AA
    
    %% Final Business Intelligence
    AA --> BB[Business Intelligence<br/>- Dimensional Analysis<br/>- OLAP Operations<br/>- Performance Dashboards<br/>- Data Warehouse Ready]
    
    %% Error Handling and Monitoring
    N --> CC[Logging System<br/>- Console Output<br/>- File Logging<br/>- Error Tracking<br/>- Performance Metrics]
    
    U --> DD[Validation Results<br/>- Data Quality Report<br/>- Integrity Check Results<br/>- Business Rule Compliance]
    
    CC --> EE[Pipeline Monitoring<br/>- Execution Status<br/>- Processing Time<br/>- Record Counts<br/>- Error Summary]
    
    DD --> EE
    EE --> BB
    
    %% SCD Type 1 Process Detail
    G --> FF[SCD Type 1 Logic<br/>- Check Existing Records<br/>- Insert New Records<br/>- Update Changed Records<br/>- Return Key Mappings]
    FF --> M
    
    %% Fact Loading Process Detail
    H --> GG[Fact Loading Logic<br/>- Lookup Dimension Keys<br/>- Validate FK Integrity<br/>- Calculate Measures<br/>- Batch Insert Records]
    GG --> Z
    
    %% Data Quality Framework
    F --> HH[Data Quality Checks<br/>- Format Validation<br/>- Business Rule Compliance<br/>- Completeness Verification<br/>- Consistency Validation]
    HH --> L
    
    %% Performance Optimization
    J --> II[Performance Features<br/>- Clustered Indexes<br/>- Foreign Key Indexes<br/>- Query Optimization<br/>- Batch Processing]
    II --> AA
    
    %% Styling
    classDef source fill:#e3f2fd,stroke:#1976d2,stroke-width:2px
    classDef config fill:#fff8e1,stroke:#f57c00,stroke-width:2px
    classDef extract fill:#f3e5f5,stroke:#7b1fa2,stroke-width:2px
    classDef transform fill:#e8f5e8,stroke:#388e3c,stroke-width:2px
    classDef dimension fill:#e0f2f1,stroke:#00695c,stroke-width:3px
    classDef fact fill:#fff3e0,stroke:#ef6c00,stroke-width:3px
    classDef orchestration fill:#ffebee,stroke:#c62828,stroke-width:3px
    classDef database fill:#e1f5fe,stroke:#0277bd,stroke-width:3px
    classDef analytics fill:#fce4ec,stroke:#ad1457,stroke-width:2px
    classDef monitoring fill:#f1f8e9,stroke:#558b2f,stroke-width:2px
    classDef validation fill:#fff9c4,stroke:#9e9d24,stroke-width:2px
    
    class A,B,C source
    class D config
    class E extract
    class F,HH transform
    class G,FF dimension
    class H,GG fact
    class N,O,P,Q,R,S,T orchestration
    class I,J,V,W,X,Y,Z,II database
    class AA,BB analytics
    class CC,EE monitoring
    class U,DD validation
    class K,L,M data
```

## Component Descriptions

### **Data Sources**
- **customers.csv**: Customer dimension source data with basic customer information
- **products.json**: Product dimension source data with nested product specifications
- **SQL Server**: Baseline sales transaction data from existing operational system

### **Core Python Modules**

#### **1. config.py - Configuration Management**
- `StarSchemaConfig` class managing all pipeline settings
- Database connection configuration with environment variables
- Star schema table definitions and mappings
- Processing parameters and validation settings

#### **2. extract.py - Data Extraction**
- `DataExtractor` class with multi-source extraction capabilities
- CSV extraction with validation and error handling
- JSON extraction supporting nested data structures
- SQL Server extraction with parameterized queries
- Sales rep extraction derived from transaction data

#### **3. transform.py - Data Transformation**
- `StarSchemaTransformer` class implementing business logic
- Customer data standardization (names, emails, phones)
- Product data enrichment (price tiers, categories)
- Sales rep dimension preparation from transaction data
- Fact measure calculations and date key generation

#### **4. load_dimensions.py - Dimension Loading**
- `DimensionLoader` class with SCD Type 1 implementation
- Surrogate key management for all dimensions
- UPSERT operations (insert new, update existing)
- Date dimension population with calendar attributes
- Business key to surrogate key mapping

#### **5. load_facts.py - Fact Loading**
- `FactLoader` class managing fact table operations
- Dimension key lookups for foreign key resolution
- Referential integrity validation before loading
- Fact measure calculations and business rule validation
- Batch processing for efficient large dataset handling

#### **6. main.py - Pipeline Orchestration**
- `StarSchemaETLPipeline` class coordinating all phases
- Sequential execution with proper dependency management
- Comprehensive error handling and recovery
- Progress tracking and performance monitoring
- Results reporting and validation

#### **7. setup_star_schema.py - Database Setup**
- `StarSchemaBuilder` class creating complete star schema
- Dimension and fact table creation with proper relationships
- Foreign key constraints and referential integrity
- Performance indexes for analytical queries
- Date dimension pre-population (2020-2025)

### **Star Schema Database Structure**

#### **Dimension Tables (4)**
- **dim_customer**: Customer dimension with surrogate keys
- **dim_product**: Product dimension with business attributes
- **dim_date**: Pre-populated calendar dimension
- **dim_sales_rep**: Sales representative dimension

#### **Fact Table (1)**
- **fact_sales**: Central fact table with foreign keys to all dimensions
- Measures: quantity, prices, discounts, calculated amounts
- Foreign keys: customer_key, product_key, date_key, sales_rep_key

### **Key ETL Processes**

#### **SCD Type 1 (Slowly Changing Dimensions)**
- Check for existing records using business keys
- Insert new records with generated surrogate keys
- Update existing records with new attribute values
- Maintain referential integrity throughout process

#### **Fact Loading with Key Lookups**
- Transform business keys to surrogate keys
- Validate all foreign key relationships exist
- Calculate derived measures (gross, net, profit amounts)
- Batch insert for performance optimization

#### **Data Quality Framework**
- Format validation at extraction
- Business rule compliance during transformation
- Referential integrity validation before fact loading
- Completeness and consistency checks throughout pipeline

### **Analytics Capabilities**

#### **Dimensional Analysis Queries**
- Sales performance by customer segment
- Product performance by category and brand
- Time-based trend analysis (monthly, quarterly, yearly)
- Cross-dimensional analysis (customer + product + time)

#### **Business Intelligence Ready**
- Optimized star schema for OLAP operations
- Performance indexes for analytical queries
- Proper foreign key relationships for joins
- Pre-aggregated measures in fact table

### **Monitoring and Validation**

#### **Pipeline Monitoring**
- Real-time progress tracking during execution
- Performance metrics collection (timing, record counts)
- Error tracking and classification
- Success/failure reporting with detailed metrics

#### **Data Validation**
- Row count validation across all tables
- Foreign key integrity verification
- Business rule compliance checking
- Data quality scoring and reporting

## Usage Instructions

### **1. Database Setup**
```bash
python solution-resized/setup_star_schema.py
```

### **2. ETL Pipeline Execution**
```bash
python solution-resized/main.py
```

### **3. Individual Component Testing**
```bash
# Test extraction
python solution-resized/extract.py

# Test transformation
python solution-resized/transform.py

# Test dimension loading
python solution-resized/load_dimensions.py

# Test fact loading
python solution-resized/load_facts.py
```

### **4. Configuration Management**
```bash
# Test configuration
python solution-resized/config.py
```

## Educational Value

This Star Schema ETL solution provides students with:

- **Data Warehouse Design** experience with proper dimensional modeling
- **ETL Best Practices** including SCD handling and fact loading patterns
- **Production Patterns** like surrogate key management and referential integrity
- **Performance Optimization** through proper indexing and batch processing
- **Error Handling** and monitoring in enterprise ETL environments
- **Analytical Query Skills** for business intelligence applications

The solution bridges the gap between academic ETL concepts and real-world data warehouse implementation, preparing students for data engineering roles requiring dimensional modeling expertise.
