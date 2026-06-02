# 🛒 Retail Sales Analytics Pipeline

## End-to-End Azure Databricks Retail Analytics Pipeline

An end-to-end Data Engineering and Business Intelligence project built using Azure Data Lake, Databricks, PySpark, PostgreSQL, and Power BI.

The pipeline ingests raw retail sales data, performs data cleaning and transformation using the Medallion Architecture (Bronze → Silver → Gold), loads curated data into PostgreSQL, and generates interactive business dashboards in Power BI.

---

## 🚀 Project Overview

This project demonstrates a complete modern data engineering workflow:

Raw Data
↓
Azure Data Lake Storage Gen2
↓
Databricks (PySpark)
↓
Bronze Layer
↓
Silver Layer
↓
Gold Layer
↓
PostgreSQL
↓
Power BI Dashboard

---

## 🎯 Business Objectives

- Process retail sales transaction data
- Clean and validate raw records
- Remove duplicate transactions
- Standardize date and pricing formats
- Generate business-ready KPI datasets
- Build executive-level Power BI dashboards
- Provide actionable business insights

---

## 🛠 Technology Stack

| Technology | Purpose |
|------------|----------|
| Azure Data Lake Gen2 | Data Storage |
| Azure Databricks | Data Processing |
| PySpark | ETL Transformations |
| PostgreSQL | Data Warehouse |
| Power BI | Dashboard & Reporting |
| Git & GitHub | Version Control |
| Python | Data Engineering |

---

## 📂 Repository Structure

```text
Retail-Sales-Analytics-Pipeline
│
├── Code
│   ├── 00_ADLS_Config.py
│   ├── 01_Bronze_To_Silver.py
│   ├── 02_Silver_To_Gold.py
│   ├── 03_Load_To_SQL.py
│   └── 04_Data_Validation.py
│
├── Dataset
│   ├── Raw_Data
│   │   ├── retail_data1.csv
│   │   ├── retail_data2.csv
│   │   └── product_details.csv
│   │
│   └── Gold_Data
│       ├── total_revenue.csv
│       ├── total_orders.csv
│       ├── average_order_value.csv
│       ├── revenue_by_city.csv
│       ├── revenue_by_category.csv
│       ├── product_performance.csv
│       ├── payment_analysis.csv
│       ├── monthly_sales.csv
│       └── category_city_sales.csv
│
├── Documentation
│   ├── Project_Documentation.docx
│   └── Notebook_Output_Screenshots
│
├── PowerBI
│   └── Retail_Sales_Analytics_Dashboard.pbix
│
└── README.md
```

---

## ⚙️ ETL Workflow

### Step 1 – Data Ingestion

- Load raw CSV files
- Store data in Azure Data Lake
- Create Bronze Layer

### Step 2 – Data Cleaning

- Remove duplicates
- Handle null values
- Standardize dates
- Validate price information
- Mask sensitive data

### Step 3 – Business Transformation

Generate Gold Layer KPI tables:

- Total Revenue
- Total Orders
- Average Order Value
- Revenue by City
- Revenue by Category
- Product Performance
- Payment Method Analysis
- Monthly Revenue Trend

### Step 4 – Data Warehouse Loading

Load Gold Layer datasets into PostgreSQL for analytics consumption.

### Step 5 – Dashboard Development

Create executive-level dashboards using Power BI.

---

## 📊 Dashboard Highlights

### Executive Dashboard

- Total Revenue
- Total Orders
- Average Order Value
- Product Categories
- Revenue by Category
- Revenue by Payment Method
- Top Products by Revenue

### Revenue Analysis Dashboard

- Revenue by City
- Revenue by Category
- Monthly Revenue Trend
- Business Insights

---

## 📈 Project Metrics

| Metric | Value |
|----------|----------|
| Raw Records Processed | 8,494 |
| Clean Records | 7,914 |
| Duplicates Removed | 580 |
| Gold KPI Tables | 9 |
| Dashboard Pages | 2 |
| Total Revenue Generated | ₹1.50 Billion |
| Top Category | Electronics (58%) |

---

## 📷 Project Screenshots

Screenshots of Databricks execution and Power BI dashboards are available in:

```text
Documentation/Notebook_Output_Screenshots
```

---

## 📊 Power BI Dashboard

Dashboard file:

```text
PowerBI/Retail_Sales_Analytics_Dashboard.pbix
```

To view:

1. Download the repository
2. Open the PBIX file using Power BI Desktop
3. Explore interactive dashboards

---

## ▶️ How to Run

### Clone Repository

```bash
git clone https://github.com/your-username/Retail-Sales-Analytics-Pipeline.git
```

### Install Dependencies

```bash
pip install pyspark pandas sqlalchemy psycopg2-binary
```

### Execute Pipeline

```bash
python 01_Bronze_To_Silver.py
python 02_Silver_To_Gold.py
python 03_Load_To_SQL.py
python 04_Data_Validation.py
```

---

## 🎓 Academic Information

Submitted By:
Ankit Kumar

Department of Information Technology

Haldia Institute of Technology

Submitted To:
NeoStats

June 2026

---
