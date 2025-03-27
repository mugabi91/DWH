# Data Warehouse (DWH) Project

## Overview
This project is a Data Warehouse (DWH) implementation built entirely using Microsoft SQL Server (T-SQL). It follows a structured multi-layered architecture where data flows systematically from raw ingestion to a refined, structured data model.

## Data Flow and Architecture
The data flows through three distinct layers:

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

## Author
Mugabi Trevor L

## License
This project is open-source and available under the [MIT License](LICENSE).

