# Banking Data Warehouse ETL Pipeline

## Introduction
This project is an implementation of a **Data Warehouse and ETL Pipeline** designed to support data integration and analytics needs in the **banking industry**. The system consolidates data from multiple heterogeneous sources, including **Excel files, CSV files, and SQL Server databases**, into a centralized **Data Warehouse using BigQuery**. The project includes the design of a **Star Schema based Data Warehouse**, the development of **ETL processes using Python**, and the creation of **Stored Procedures** to support analytical and reporting requirements.

## Objectives
- Built a **Data Warehouse** using **BigQuery**
- Developed **ETL (Extract, Transform, Load)** processes to integrate data from multiple sources with **deduplication mechanisms** to ensure data quality and consistency
- Created **Stored Procedures** to support fast data analysis and reporting

## Tools & Technologies
- Python  
- BigQuery  
- SQL Server  

## Data Architecture

### Data Sources
- Excel files  
- CSV files  
- SQL Server Database:
  - account  
  - customer  
  - branch  
  - city  
  - state  
  - transaction_db  

### Data Warehouse Design
The Data Warehouse is designed using a **Star Schema**, consisting of:
- **Dimension Tables**
  - DimAccount  
  - DimCustomer  
  - DimBranch  
- **Fact Table**
  - FactTransaction  

## ETL Process

### 1. Extract
- Extracted data from **Excel, CSV, and SQL Server**

### 2. Transform

#### 2.1 Dimension Account Transformation
- Mapped source columns to **PascalCase** format  
- Standardized `AccountType` and `Status` values using **title case**  
- Converted the `DateOpened` column to **datetime** format  

#### 2.2 Dimension Branch Transformation
- Mapped source columns to **PascalCase** format  

#### 2.3 Dimension Customer Transformation
- Mapped source columns to **PascalCase** format  
- Performed a join between the **customer** and **city** tables using `city_id`, then joined the result with the **state** table using `state_id`  
- Standardized `Gender` values using **title case**  
- Consolidated customer demographic and location attributes into a single dimension table  

#### 2.4 Fact Transaction Transformation
- Mapped source columns to **PascalCase** format  
- Combined all transaction data sources into a single dataset  
- Applied **deduplication based on `TransactionID`** to prevent duplicate records  
- Converted the `TransactionDate` column to **datetime** format  

### 3. Load
- Loaded data into **dimension and fact tables** in the Data Warehouse  


## Use Case

### Daily Transaction Summary
The **DailyTransaction** stored procedure is designed to provide a daily summary of transaction activity within a specified date range. It aggregates transaction data directly from the **FactTransaction** table to calculate the total number of transactions and the total transaction amount per day.

This stored procedure accepts `start_date` and `end_date` parameters, enabling users to retrieve transaction summaries for a specific period efficiently without querying raw transactional data.

**Output:**
- Date  
- TotalTransactions  
- TotalAmount  
