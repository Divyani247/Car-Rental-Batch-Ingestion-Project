# Car-Rental-Batch-Ingestion-Project
## 📋 Project Overview

This project demonstrates a comprehensive **batch data processing pipeline** for car rental analytics using **Apache Airflow**, **Google Cloud Dataproc**, **Apache Spark**, and **Snowflake**. The pipeline processes daily car rental transaction data and implements **SCD Type 2** (Slowly Changing Dimension) for customer data management.

## Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   GCS Storage   │───▶│  Apache Airflow  │───▶│ Google Dataproc │───▶│   Snowflake     │
│                 │    │                  │    │                 │    │                 │
│ • JSON Files    │    │ • DAG Orchestr.  │    │ • Spark Jobs    │    │ • Data Warehouse│
│ • CSV Files     │    │ • SCD2 Logic     │    │ • Data Transform│    │ • Analytics     │
└─────────────────┘    └──────────────────┘    └─────────────────┘    └─────────────────┘
```

## 🎯 Key Features

- **🔄 Batch Processing**: Daily data ingestion with parameterized execution dates
- **📊 SCD Type 2**: Slowly Changing Dimension implementation for customer data
- **⚡ Spark Processing**: Large-scale data transformation using PySpark
- **🎛️ Airflow Orchestration**: Workflow management with dependency handling
- **☁️ Cloud Integration**: Google Cloud Storage and Dataproc integration
- **❄️ Snowflake Analytics**: Data warehouse with dimensional modeling

## 📁 Project Structure

```
Car-Rental-Batch-Ingestion-Project/
├── airflow_job/
│   └── car_rental_airflow_dag.py          # Airflow DAG for orchestration
├── spark_job/
│   └── spark_job.py                       # PySpark data processing job
├── data/
│   ├── car_rental_20250903.json          # Sample rental data (Sept 3, 2025)
│   ├── car_rental_20250904.json          # Sample rental data (Sept 4, 2025)
│   ├── customers_20250903.csv            # Customer data (Sept 3, 2025)
│   └── customers_20250904.csv            # Customer data (Sept 4, 2025)
├── jar_files/
│   ├── snowflake-jdbc-3.16.0.jar         # Snowflake JDBC connector
│   └── spark-snowflake_2.12-2.15.0-spark_3.4.jar  # Spark-Snowflake connector
├── snowflake_dwh_setup.sql               # Data warehouse setup script
└── README.md                             # This documentation
```

##  Demo Screenshot
<img width="1879" height="732" alt="car_rental_project" src="https://github.com/user-attachments/assets/d2371eac-5ae8-45e0-89b9-1bc4c94cfd8e" />

<img width="1920" height="856" alt="car_rental_project_2" src="https://github.com/user-attachments/assets/d8cb3db8-5a7c-45ed-adee-7ae4c784fd2a" />

## 🗄️ Data Model

### **Dimensional Model (Star Schema)**

#### **Dimension Tables:**
- **`location_dim`**: Airport/city locations (10 locations)
- **`car_dim`**: Vehicle information (10 car models)
- **`date_dim`**: Calendar dates (September 2025)
- **`customer_dim`**: Customer data with SCD Type 2 support

#### **Fact Table:**
- **`rentals_fact`**: Daily rental transactions with foreign keys to all dimensions

### **SCD Type 2 Implementation:**
- **`effective_date`**: When the record became active
- **`end_date`**: When the record was superseded (NULL for current)
- **`is_current`**: Boolean flag for current records

## 🔧 Technical Components

### **1. Apache Airflow DAG (`car_rental_airflow_dag.py`)**

**Features:**
- **Parameterized Execution**: Accepts date parameter (yyyymmdd format)
- **SCD2 Logic**: Handles customer dimension updates
- **Task Dependencies**: Sequential execution with proper ordering
- **Error Handling**: Retry logic and failure notifications

**Task Flow:**
```
get_execution_date → merge_customer_dim → insert_customer_dim → submit_pyspark_job
```

**Key Tasks:**
- **`get_execution_date`**: Resolves execution date from parameters
- **`merge_customer_dim`**: Closes out changed customer records (SCD2)
- **`insert_customer_dim`**: Inserts new customer versions
- **`submit_pyspark_job`**: Triggers Spark job for fact table processing

### **2. PySpark Job (`spark_job.py`)**

**Data Processing Pipeline:**
1. **Data Ingestion**: Reads JSON files from GCS
2. **Data Validation**: Filters out incomplete records
3. **Data Transformation**: Calculates derived fields
4. **Dimension Joins**: Enriches data with surrogate keys
5. **Fact Table Load**: Writes processed data to Snowflake

**Key Transformations:**
- **Rental Duration**: Calculates days between start and end dates
- **Total Amount**: Multiplies amount by quantity
- **Daily Average**: Calculates average daily rental cost
- **Long Rental Flag**: Identifies rentals > 7 days

### **3. Snowflake Data Warehouse (`snowflake_dwh_setup.sql`)**

**Setup Components:**
- **Database Creation**: `car_rental` database
- **Table Definitions**: All dimension and fact tables
- **Sample Data**: Pre-populated dimension data
- **Storage Integration**: GCS integration for file access
- **External Stage**: Stage for customer CSV files

## 📊 Sample Data

### **Rental Data Structure:**
```json
{
    "rental_id": "RNTL001",
    "customer_id": "CUST001",
    "car": {
        "make": "Tesla",
        "model": "Model S",
        "year": 2022
    },
    "rental_period": {
        "start_date": "2025-09-02",
        "end_date": "2025-09-08"
    },
    "rental_location": {
        "pickup_location": "New York - JFK Airport",
        "dropoff_location": "Denver - DEN Airport"
    },
    "amount": 255.21,
    "quantity": 1
}
```

### **Customer Data Structure:**
```csv
customer_id,name,email,phone
CUST001,John Smith,john.smith@email.com,+1-555-0101
CUST002,Jane Doe,jane.doe@email.com,+1-555-0102
```

## 🔍 Data Quality & Validation

### **Validation Rules:**
- **Mandatory Fields**: rental_id, customer_id, car details, dates, locations, amounts
- **Data Types**: Proper type conversion and validation
- **Business Rules**: Rental duration calculations, amount validations
- **Referential Integrity**: Foreign key constraints in Snowflake

### **Error Handling:**
- **Airflow**: Retry logic with exponential backoff
- **Spark**: Data validation with filtering of invalid records
- **Snowflake**: Constraint validation and error logging

## 📈 Analytics & Insights

### **Key Metrics:**
- **Revenue Analysis**: Total rental amounts by location, car type, time period
- **Customer Behavior**: Rental patterns, duration analysis
- **Operational Metrics**: Pickup/dropoff location analysis
- **Performance KPIs**: Average daily rental amounts, long rental trends

### **Sample Queries:**
```sql
-- Revenue by location
SELECT l.location_name, SUM(rf.total_rental_amount) as total_revenue
FROM rentals_fact rf
JOIN location_dim l ON rf.pickup_location_key = l.location_key
GROUP BY l.location_name
ORDER BY total_revenue DESC;

-- Customer rental patterns
SELECT c.name, COUNT(*) as rental_count, AVG(rf.rental_duration_days) as avg_duration
FROM rentals_fact rf
JOIN customer_dim c ON rf.customer_key = c.customer_key
WHERE c.is_current = TRUE
GROUP BY c.name
ORDER BY rental_count DESC;
```
## 📚 Learning Objectives

This project demonstrates:

1. **Batch Processing Patterns**: Daily data ingestion workflows
2. **SCD Implementation**: Slowly Changing Dimension Type 2
3. **Cloud Data Engineering**: GCP, Airflow, and Snowflake integration
4. **Dimensional Modeling**: Star schema design and implementation
5. **Data Validation**: Quality checks and error handling
6. **Orchestration**: Workflow management with Airflow
7. **Scalable Processing**: Spark-based data transformation

## 🎯 Business Value

- **Operational Efficiency**: Automated daily data processing
- **Data Quality**: Comprehensive validation and error handling
- **Scalability**: Cloud-native architecture for growth
- **Analytics Ready**: Dimensional model for business intelligence
- **Historical Tracking**: SCD2 for customer data evolution
- **Cost Optimization**: Efficient resource utilization


---

**🔧 Built with:** Apache Airflow, Google Cloud Dataproc, Apache Spark, Snowflake, Python, SQL

ref: https://growdataskills.com/
