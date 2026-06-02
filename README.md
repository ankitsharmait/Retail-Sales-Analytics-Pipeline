# 🛒 Retail Sales Analytics Pipeline
### End-to-End Azure Databricks Retail Analytics Pipeline

![Azure](https://img.shields.io/badge/Azure-Data%20Lake%20Gen2-0078D4?style=flat&logo=microsoftazure)
![Databricks](https://img.shields.io/badge/Azure-Databricks-FF3621?style=flat&logo=databricks)
![PySpark](https://img.shields.io/badge/Apache-PySpark-E25A1C?style=flat&logo=apachespark)
![PowerBI](https://img.shields.io/badge/Microsoft-Power%20BI-F2C811?style=flat&logo=powerbi&logoColor=black)
![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=flat&logo=python)
![GitHub](https://img.shields.io/badge/GitHub-Version%20Control-181717?style=flat&logo=github)

> **Submitted by:** Ankit Kumar | Department of Information Technology, Haldia Institute of Technology
> **Submitted to:** NeoStats · June 2026

---

## 📌 Project Highlights

| Metric | Value |
|--------|-------|
| 📦 Raw Records Processed | 8,494 |
| ✅ Clean Records (Silver Layer) | 7,914 |
| 🗑️ Duplicates Removed | 580 |
| 📊 Gold KPI Aggregation Tables | 9 |
| 📈 Power BI Dashboard Pages | 5 |
| ☁️ Azure Cloud Services Used | 3 |
| 💰 Total Revenue Generated | ₹1,501M |
| 🏆 Top Category Revenue Share | 58% (Electronics) |

---

## 📁 Repository Structure

```
Retail-Sales-Analytics-Pipeline/
│
├── Code/
│   ├── 00_ADLS_Config.py          # Azure Data Lake Storage configuration
│   ├── 01_Bronze_To_Silver.py     # Data cleaning & transformation pipeline
│   ├── 02_Silver_To_Gold.py       # KPI aggregation to Gold layer
│   ├── 03_Load_To_SQL.py          # Load Gold data to SQL
│   └── 04_Data_Validation.py      # Schema & data quality validation
│
├── Dataset/                        # Source Excel/CSV files
│
├── Documentation/
│   ├── Notebook_Output_Screenshots/
│   └── Project_Documentation.docx
│
├── PowerBI/                        # Power BI dashboard files (.pbix)
│
└── README.md
```

---

## 🏗️ Architecture Overview

This project follows the **Medallion Architecture** — a multi-layered data quality framework:

```
Excel/CSV Source Files
        │
        ▼
ADLS Gen2 — Bronze Layer      ← Raw data, unmodified
        │
        ▼
Azure Databricks (PySpark)    ← Cleaning, deduplication, PII masking
        │
        ▼
ADLS Gen2 — Silver Layer      ← 7,914 clean records
        │
        ▼
ADLS Gen2 — Gold Layer        ← 9 aggregated KPI tables
        │
        ▼
Microsoft Power BI             ← 5 interactive dashboard pages
```

---

## 🛠️ Technology Stack

| Technology | Purpose | Version / Tier |
|---|---|---|
| **Azure Data Lake Gen2** | Scalable cloud storage (Bronze / Silver / Gold) | Standard LRS / Hot Tier |
| **Azure Databricks** | Distributed PySpark processing platform | Runtime 13.3 LTS |
| **Apache PySpark** | Data transformation & aggregation engine | PySpark 3.4+ |
| **Microsoft Power BI** | Interactive BI dashboards & reporting | Desktop / Service |
| **Python** | Data engineering scripts & utilities | Python 3.10+ |
| **SQL (Spark SQL)** | Validation, ad-hoc analysis & aggregations | Spark SQL 3.x |
| **GitHub** | Version control & repository management | Cloud |

---

## 📂 Dataset Description

Three datasets form the complete data model for this pipeline:

### product_details (Dimension Table)

| Column | Data Type | Description | Example |
|--------|-----------|-------------|---------|
| product_id 🔑 | String | Unique product identifier | P001 |
| product_name | String | Standardized name | Laptop Pro |
| category | String | Product category | Electronics |
| standard_price | Decimal | Reference unit price | ₹45,000.00 |

### retail_df (Fact Table — Combined Transactions)

| Column | Data Type |
|--------|-----------|
| transaction_id 🔑 | Integer |
| customer_id | Integer |
| customer_name | String |
| product_id 🔗 (FK → product_details) | Integer |
| product_name | String |
| category | String |
| price | Integer |
| quantity | Integer |
| discount | Double |
| purchase_location | String |
| city | String |
| payment_method | String |
| payment_status | String |
| email | String |
| phone | Long |
| transaction_date | Date (YYYY-MM-DD) |

### Transaction Sources

| Attribute | Retail Data 1 | Retail Data 2 |
|-----------|---------------|---------------|
| Total Records | 4,243 | 4,251 |
| Source System | System 1 (Online) | System 2 (Offline) |
| Known Issues | Duplicates, nulls, format issues | Duplicates, nulls, format issues |

---

## ⚙️ Pipeline Stages

| # | Stage | Description |
|---|-------|-------------|
| 1 | **Data Ingestion** | Source Excel files uploaded to ADLS Gen2; PySpark reads into Databricks |
| 2 | **Schema Validation** | Column names, types, and counts validated against expected schemas |
| 3 | **Bronze Load** | Raw data stored in Bronze container without modification |
| 4 | **Deduplication** | Duplicate records identified by Transaction ID and removed |
| 5 | **Null Handling** | Missing price, quantity, and category resolved via product_dim |
| 6 | **Standardization** | Product names, dates, and categorical fields standardized |
| 7 | **PII Masking** | Customer email and phone masked before Silver write |
| 8 | **Silver Load** | Cleaned data written to Silver container |
| 9 | **Gold Aggregation** | Business KPIs computed and written to Gold layer tables |
| 10 | **Power BI** | Gold datasets imported into Power BI for visualization |

---

## 🧹 Data Cleaning & Transformation

### Problems Addressed

- **Duplicate Transactions** — Same transaction from multiple source systems inflating revenue
- **Missing Values** — Null prices, quantities, and categories filled via dimension lookups
- **Invalid Quantities** — Zero/negative quantity records removed
- **Inconsistent Product Names** — `Laptop` / `laptop` / `LAPTOP` → standardized via product_dim
- **Mixed Date Formats** — `DD/MM/YYYY`, `MM-DD-YYYY` → all standardized to `YYYY-MM-DD`
- **PII Exposure** — Email and phone numbers exposed in plain text

### Results

| Metric | Value |
|--------|-------|
| Total Raw Records | 8,494 |
| Duplicates Removed | 580 |
| Final Clean Records | 7,914 |
| Product Standardization | 100% (all 10 products mapped) |
| Total Revenue Generated | ₹1,501M |
| Electronics Revenue Share | 58% (Top Category) |

### Missing Value Strategy

| Field | Resolution Strategy |
|-------|---------------------|
| Unit Price | Lookup from product_dim by Product ID |
| Total Amount | Recalculate: `Qty × Price × (1 − Discount)` |
| Category | Lookup from product_dim |
| Discount | Default to `0.0` |
| Payment Method | Flag as `'Unknown'` |

### Date Standardization

| Original Format | Example | Standardized To |
|-----------------|---------|-----------------|
| DD/MM/YYYY | 15/07/2025 | 2025-07-15 |
| MM-DD-YYYY | 07-15-2025 | 2025-07-15 |
| YYYY-MM-DD | 2025-07-15 | 2025-07-15 (no change) |

---

## 🔒 PII Masking Strategy

| Field | Original Value | Masked Value | Rule |
|-------|---------------|--------------|------|
| Email | ankit@gmail.com | an\*\*\*@gmail.com | SHA-256 Hash |
| Phone | 9876543210 | 98\*\*\*\*\*\*10 | First 2 + `******` + Last 2 digits |

> Masking applied via **PySpark UDFs** before Silver layer write. Original values are never stored in Silver or Gold containers.

---

## 📊 Gold Layer — KPI Tables

| Table Name | Description | Key Columns |
|------------|-------------|-------------|
| `total_revenue` | Overall business revenue summary | total_revenue, period |
| `total_orders` | Count of all valid transactions | total_orders, period |
| `average_order_value` | Mean transaction value | avg_order_value |
| `revenue_by_category` | Revenue by product category | category, revenue |
| `revenue_by_city` | Revenue by city/geography | city, revenue |
| `product_performance` | Per-product revenue ranking | product, revenue, rank |
| `payment_analysis` | Payment method distribution | payment_method, share |
| `monthly_sales` | Month-over-month revenue trend | month, year, revenue |
| `category_city_sales` | Category × City cross analysis | category, city, revenue |

---

## 📐 Business KPIs

| KPI | Formula | Result |
|-----|---------|--------|
| Total Revenue | `SUM(qty × price × (1 − discount))` | ₹1,501M |
| Total Orders | `COUNT(transaction_id)` | 7,914 |
| Average Order Value | `Total Revenue / Total Orders` | — |
| Revenue by Category | `GROUP BY category, SUM(revenue)` | Electronics: 58% |
| Revenue by City | `GROUP BY city, SUM(revenue)` | — |
| Product Performance | `GROUP BY product, RANK()` | — |
| Payment Method Share | `GROUP BY method, COUNT(*)` | — |
| Monthly Revenue Trend | `GROUP BY year, month, SUM(revenue)` | — |

---

## Data Model

```
product_details (Dimension)
        │
        │  product_id (Join Key)
        ▼
retail_df (retail1 + retail2 combined)
        │
        │  Cleaning + Transformation (PySpark)
        ▼
silver_df (7,914 Clean Records)
        │
        │  Gold Aggregations
        ▼
Gold Layer Tables → Power BI
  total_revenue      │  total_orders        │  average_order_value
  revenue_by_category│  revenue_by_city     │  product_performance
  payment_analysis   │  monthly_sales       │  category_city_sales
```

> The join between `product_details` and `retail_df` occurs during **Silver Layer processing in PySpark**, not inside Power BI. This is a pipeline-based architecture, not a traditional star schema.

---

## 📈 Power BI Dashboard

5 purpose-built dashboard pages consuming Gold Layer tables directly:

- **Revenue Overview** — Total revenue (₹1,501M), orders, and AOV KPI cards
- **Category Performance** — Revenue breakdown by product category (Electronics: 58%)
- **City-wise Analysis** — Geographic revenue distribution
- **Product Rankings** — Top-performing products by revenue
- **Monthly Trends** — Month-over-month revenue trend lines

---

## 🚧 Challenges Faced & Solutions

| Challenge | Solution Applied |
|-----------|-----------------|
| Duplicate records from two source systems | Transaction ID-based deduplication (580 removed) |
| Inconsistent product naming across systems | Dimension table join for canonical names |
| Multiple date formats in source files | PySpark multi-format date parser with regex |
| PII compliance for email and phone | SHA-256 hashing + phone digit masking via UDFs |
| Null prices breaking revenue calculations | product_dim lookup fallback + derived formula |

---

## 📦 ADLS Gen2 Container Structure

```
adls-retail-storage/
├── bronze/   ← Raw ingested files (unmodified, immutable)
├── silver/   ← Cleaned, validated, PII-masked data (7,914 records)
└── gold/     ← Aggregated KPI tables for Power BI (9 tables)
```

---

## 🔗 References

1. [Microsoft Azure Documentation](https://docs.microsoft.com/en-us/azure/)
2. [Azure Databricks Documentation](https://docs.databricks.com/)
3. [Apache Spark / PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
4. [Microsoft Power BI Documentation](https://docs.microsoft.com/en-us/power-bi/)
5. [Azure Data Lake Storage Gen2](https://docs.microsoft.com/en-us/azure/storage/blobs/)
6. [GitHub Documentation](https://docs.github.com/en)

---

## Conclusion

An end-to-end Azure Databricks Retail Analytics Pipeline successfully designed, developed, and deployed using **Azure Data Lake Storage Gen2**, **Azure Databricks**, **Apache PySpark**, **GitHub**, and **Microsoft Power BI**.

The solution addresses all identified data quality challenges through a structured, automated pipeline following the **Medallion Architecture**. **580 duplicate records** were removed from **8,494 raw transactions**, producing **7,914 clean, validated records**. Business KPIs — including **₹1,501M total revenue** and **58% Electronics revenue share** — were computed across 9 Gold layer tables and delivered through five purpose-built Power BI dashboard pages.

The architecture is **cloud-native, scalable, and production-ready** — providing a reliable foundation for data-driven decision making at ABC Retail Solutions.

---