# Data Warehouse (DWH) Project

## Overview

This project is a Data Warehouse (DWH) implementation built entirely using Microsoft SQL Server (T-SQL). It follows a structured multi-layered architecture where data flows systematically from raw ingestion to a refined, structured data model.

![Data Flow Chart](illustrations/DWH%20explanation.jpg)

## Data Flow and Architecture

The data flows through three distinct layers:
![Data Flow Chart 2](illustrations/Thought%20process.jpg)

1. **Bronze Layer** (Raw Data Landing Zone)
   - This is the staging layer where raw CSV files are ingested without any modifications.
   - Data remains unchanged to preserve the original source information.
2. **Silver Layer** (Cleansed and Processed Data)
   - Data is transformed to correct data types, handle missing values, and enrich metadata.
   - Any inconsistencies in the raw data are cleaned up at this stage.
3. **Gold Layer** (Optimized for Analytics & Reporting)
   - Instead of storing new tables, this layer primarily consists of **views** to optimize storage.
   - A structured **data model** is implemented to unify data from multiple sources.

## Data Sources

The project integrates data from two main systems:

### 1. Customer Relationship Management (CRM) System

- `cust_info.csv` - Contains customer details.
- `prod_info.csv` - Contains product details.
- `sales_details.csv` - Contains sales transaction records.

### 2. Enterprise Resource Planning (ERP) System

- `cust_az12.csv` - Contains additional customer information.
- `loc_a101.csv` - Contains location-based data.
- `px_cat_giv2.csv` - Contains product categorization details.

All data sources are CSV files and are loaded into SQL Server.

![Data flow chart 3](illustrations/DWH%20arch.jpg)

## Technologies Used

- **Database:** Microsoft SQL Server
- **Query Language:** T-SQL (Transact-SQL)
- **Data Storage:** CSV files as input
- **Data Processing:** SQL-based transformations and views

## Project Objectives

- Implement a structured **ETL (Extract, Transform, Load)** pipeline using T-SQL.
- Establish a **layered approach** (Bronze, Silver, Gold) for data processing.
- Optimize storage and performance using **views** instead of redundant tables in the Gold Layer.
- Provide a **scalable data model** for analytics and reporting.

## How to Use This Repository

1. Clone the repository:

   ```sh
   git clone https://github.com/mugabi91/DWH.git
   ```

2. Set up a Microsoft SQL Server instance.
3. Load the raw CSV files into the Bronze Layer.
4. Execute the T-SQL scripts to process data into the Silver and Gold layers.
5. Use the final views in the Gold Layer for reporting and analytics.

## Future Improvements

- Automate data ingestion using SQL Server Integration Services (SSIS) or Python.
- Implement incremental data loading to optimize performance.
- Add indexing strategies for faster query execution.
- Introduce data validation and monitoring mechanisms.

## Requirements for Recreating the Data Warehouse

## 1. System Requirements

- **Operating System:** Windows (Recommended) or Linux with SQL Server support
- **Database Engine:** Microsoft SQL Server (2016 or later recommended)
- **Storage:** Sufficient disk space to accommodate the raw CSV files and processed data
- **Memory:** At least 4GB RAM (Recommended: 8GB+ for better performance)

## 2. Software & Tools

- **Microsoft SQL Server** (Standard or Developer Edition)
- **SQL Server Management Studio (SSMS)** for querying and administration
- **PowerShell or Command Line** (optional, for automated file handling)
- **Git** (optional, for version control and managing scripts)

## 3. Required Data Files

Ensure you have the following CSV files from the ERP and CRM systems:

### CRM System Files

- `cust_info.csv` - Customer details
- `prod_info.csv` - Product details
- `sales_details.csv` - Sales transactions

### ERP System Files

- `cust_az12.csv` - Additional customer data
- `loc_a101.csv` - Location data
- `px_cat_giv2.csv` - Product categorization

## 4. Database Setup

### Step 1: Install SQL Server

Ensure SQL Server is installed and running. If not, download and install from [Microsoft SQL Server](https://www.microsoft.com/en-us/sql-server/).

### Step 2: Create the Database

Execute the following command in SSMS:

```sql
CREATE DATABASE DWH;
GO
```

### Step 3: Set Up Schema and Layers

Run the provided T-SQL scripts in the repository to create:

- **Bronze Layer** (Raw staging tables)
- **Silver Layer** (Transformed, cleaned tables)
- **Gold Layer** (Views and final data model)

### Step 4: Load the Data

Use `BULK INSERT` or `OPENROWSET` to load data from CSV files into the Bronze Layer.
Example:

```sql
BULK INSERT Bronze.cust_info
FROM 'replace with path to the location of your files on your pc'
WITH (
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2
);
```

### Step 5: Run Transformation Queries

Execute the scripts to clean and transform the data into the Silver Layer.

### Step 6: Validate and Query the Data Model

Once data is in the Gold Layer, use the views for analytics and reporting.

## 5. Optional Enhancements

- **Automate Data Loading:** Set up SQL Server Integration Services (SSIS) or use Python scripts.
- **Performance Optimization:** Add indexing and partitioning strategies.
- **Data Monitoring:** Implement logging and validation checks.

## 6. Troubleshooting

- Ensure SQL Server allows bulk inserts (`sp_configure 'show advanced options', 1;`)
- Check file paths and permissions for CSV import
- Verify data types during transformations to avoid errors

## 7. Author & License

- Author: Mugabi Trevor L
- License: MIT License
