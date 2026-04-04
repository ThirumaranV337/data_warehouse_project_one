Data Warehouse Implementation: Medallion Architecture
📌 Project Overview
This project demonstrates the implementation of a modern Data Warehouse following the Medallion Architecture. Inspired by the "Data with Barani" (Data with Baara) methodology, this project transforms raw source data into high-value, analytics-ready business insights using SQL Server and structured data modeling.

🏗️ Architecture: The Medallion Approach
The data flows through three distinct layers to ensure quality and reliability:

Bronze (Raw Layer): Ingests raw data from source systems (CRM, ERP, etc.) in its original format.

Silver (Cleansing Layer): Data is cleaned, standardized, and transformed. This involves handling nulls, correcting data types, and deduplication.

Gold (Curated Layer): The final layer where data is modeled into Dimensions and Fact tables (Star Schema) for business reporting.

🚀 Technical Stack
Database: SQL Server / Azure SQL Database

Language: T-SQL (Transact-SQL)

Architecture: Medallion Layers (Bronze → Silver → Gold)

Modeling: Dimensional Modeling (Star Schema)

📁 Project Structure
Bronze: Raw tables capturing original source data.

Silver: Cleaned versions of CRM and ERP data (e.g., silver.crm_sales_info).

Gold: Final business views including:

gold.dim_products: Product master data.

gold.dim_customers: Customer demographic data.

gold.fact_sales: Centralized sales transactions linked to dimensions.

🔧 Key Features
Data Transformation: Converting inconsistent date formats and currency strings into usable formats.

Relational Mapping: Connecting disparate data sources through defined primary and foreign keys.

Business Views: Using SQL views to create a semantic layer for easy reporting in tools like Power BI or Excel.

📈 Learning Outcomes
Mastered complex SQL Joins (Left, Inner) to build Fact tables.

Implemented Schema Isolation (using separate schemas for Bronze, Silver, and Gold).

Learned the importance of Data Quality checks during the transformation process.

Author: Thirumaran V

Student of B.Tech AI & DS | Aspiring Data Engineer
