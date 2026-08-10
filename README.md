# Azure End-to-End Data Engineering Project

## 📌 Project Overview

This project demonstrates an end-to-end **Azure + Databricks data engineering pipeline** that moves raw Parquet data through **Bronze, Silver, and Gold layers** using Azure Data Lake Storage Gen2 and Databricks.

The project works with three main business datasets:

- `customers`
- `orders`
- `products`

It also contains a `regions` dataset in the Silver layer.

The overall flow is:

**Source Parquet files → Bronze ingestion → Silver transformation → Gold dimensional/fact layer → Delta tables / DLT pipeline**

The project uses **Databricks notebooks, PySpark, Delta Lake, Databricks SQL, Auto Loader, Delta Live Tables (DLT), SCD concepts, and Azure Data Lake Storage Gen2**.

---

## 🏗️ Architecture

```text
                    ┌──────────────────────┐
                    │      Parameters      │
                    │   file_name / jobs   │
                    └──────────┬───────────┘
                               │
                               ▼
                 ┌─────────────────────────┐
                 │    Bronze Auto Loader   │
                 │ Bronze_Autoloader_      │
                 │ iteration               │
                 └────────────┬────────────┘
                              │
                ┌─────────────┼─────────────┐
                │             │             │
                ▼             ▼             ▼
        ┌────────────┐ ┌────────────┐ ┌────────────┐
        │   Silver   │ │   Silver   │ │   Silver   │
        │ Customers  │ │   Orders   │ │  Products  │
        └─────┬──────┘ └─────┬──────┘ └─────┬──────┘
              │              │              │
              └──────────────┼──────────────┘
                             │
                             ▼
                    ┌───────────────────┐
                    │       Gold        │
                    │ Dim Customers     │
                    │ Dim Products      │
                    └─────────┬─────────┘
                              │
                              ▼
                       ┌─────────────┐
                       │ Fact Orders │
                       └─────────────┘
```

### Layered architecture

| Layer | Purpose | Technology |
|---|---|---|
| **Source** | Raw Parquet input files | Azure Data Lake Storage |
| **Bronze** | Ingest raw data from source | Databricks Auto Loader / Structured Streaming |
| **Silver** | Clean and transform data | PySpark + Delta Lake |
| **Gold** | Business-ready dimensions and fact table | Delta Lake + DLT |
| **Catalog** | Register/query Delta tables | Databricks Catalog / Schemas |

---

# 📂 Source Data

The project contains the following Parquet files:

```text
customer_first.parquet
customers_second.parquet

orders_first.parquet
orders_second.parquet

products_first.parquet
products_second.parquet

regions.parquet
```

The presence of first/second files allows the project to demonstrate processing multiple data batches/iterations.

---

# 🥉 Bronze Layer

The Bronze layer is responsible for ingesting the source Parquet files into Azure Data Lake Storage.

The `Bronze_Layer.py` notebook is parameterized using a Databricks widget:

```python
dbutils.widgets.text("file_name","")
p_file_name = dbutils.widgets.get("file_name")
```

It then uses **Databricks Auto Loader** with the `cloudFiles` format to read Parquet data:

```python
df = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .option("cloudFiles.schemaLocation", ...) \
    .load(...)
```

The data is written to the Bronze storage location using Structured Streaming with:

- `append` mode
- checkpointing
- `trigger(once=True)`

This allows the same ingestion notebook to be reused for different datasets by passing the dataset/file name as a parameter.

The notebook also reads the Bronze data back and displays it for validation.

Source: `Bronze_Layer.py`

---

# 🥈 Silver Layer

The Silver layer contains cleaned and transformed Delta data.

## 1. Silver Customers

Notebook: `Silver_customers.py`

### Transformations

The customer data is read from the Bronze layer and:

1. Removes `_rescued_data`
2. Extracts the email domain using `split()`
3. Groups customers by email domain for analysis
4. Creates a `Full_name` column using first and last name
5. Removes the original `first_name` and `last_name` columns
6. Writes the result as a Delta dataset
7. Registers the Delta location as:

```text
project_cata.silver.customers_silver
```

The Delta table is created over the Silver storage location.

Source: `Silver_customers.py`

---

## 2. Silver Orders

Notebook: `Silver_orders.py`

### Transformations

The orders data is:

1. Read from the Bronze layer
2. Inspected using `display()` and `printSchema()`
3. Cleaned by removing `_rescued_data`
4. A `year` column is derived from `order_date`
5. Window functions are demonstrated using:
   - `dense_rank()`
   - `rank()`
   - `row_number()`
6. The transformed data is written as Delta
7. Registered as:

```text
project_cata.silver.orders_silver
```

The notebook also demonstrates encapsulating window operations inside a Python class.

Source: `Silver_orders.py`

---

## 3. Silver Products

Notebook: `silver_products.py`

### Transformations

The products data is:

1. Read from Bronze
2. `_rescued_data` is removed
3. A SQL UDF named `discount_func` is created
4. The UDF calculates a 10% discount:

```text
discounted_price = price * 0.90
```

5. The UDF is used from both SQL and PySpark
6. The result is stored as Delta
7. Registered as:

```text
project_cata.silver.products_silver
```

Source: `silver_products.py`

---

## 4. Silver Regions

Notebook: `silver_Region.py`

The regions data is read from the Databricks Bronze schema, `_rescued_data` is removed, and the result is written as Delta to:

```text
abfss://silver@projecte2e.dfs.core.windows.net/regions
```

It is then registered as:

```text
project_cata.silver.regions_silver
```

Source: `silver_Region.py`

---

# 🥇 Gold Layer

The Gold layer contains business-ready dimensional and fact structures.

The project creates:

```text
DimCustomers
DimProducts
FactOrders
```

---

# 👤 Gold Customers — SCD Type 1

Notebook: `Gold_customers.py`

The customer Silver table is used as the source.

### Processing steps

1. Read `project_cata.silver.customers_silver`
2. Remove duplicate records using `customer_id`
3. Compare incoming records with existing Gold customer records
4. Separate new and existing records
5. Generate a surrogate key:
   - `DimCustomerKey`
6. Add/create `create_date` and `update_date`
7. Combine new and existing records
8. Apply an **SCD Type 1** merge into:

```text
project_cata.gold_schema.DimCustomers
```

The merge uses Delta Lake and updates matched records while inserting new records.

### SCD Type 1 behavior

When an existing customer's attribute changes, the current record is updated rather than preserving historical versions.

Source: `Gold_customers.py`

---

# 📦 Gold Products — DLT / SCD Type 2

Notebook: `Gold_Products.py`

The product Gold layer is implemented using **Delta Live Tables (DLT)**.

### Data quality expectations

Two rules are defined:

```python
product_id IS NOT NULL
product_name IS NOT NULL
```

Records violating the defined expectations are handled using the DLT expectation configuration.

### DLT flow

```text
Silver Products
      │
      ▼
DimProducts_stage
      │
      ▼
Dimproducts_view
      │
      ▼
DimProducts
```

The pipeline uses:

```python
dlt.create_streaming_table("DimProducts")
```

and:

```python
dlt.apply_changes(
    target="DimProducts",
    source="Live.DimProducts_view",
    keys=["product_id"],
    sequence_by="product_id",
    stored_as_scd_type=2
)
```

This demonstrates an **SCD Type 2** implementation, where changes can be represented as historical versions of a dimension record.

Source: `Gold_Products.py`

---

# 🧾 Fact Orders

Notebook: `Gold Orders.py`

The Fact Orders table combines order data with customer and product dimensions.

### Source

Orders are read from:

```text
project_cata.silver.orders_silver
```

Customer dimension keys are obtained from:

```text
project_cata.gold_schema.dimcustomers
```

Product dimension information is obtained from the product dimension table.

### Join process

The orders are joined with:

- Customer dimension
- Product dimension

using their respective business keys.

The temporary join columns are then removed.

### Final table

The resulting dataset is written to:

```text
abfss://gold@projecte2e.dfs.core.windows.net/Factorders
```

and registered as:

```text
project_cata.gold_schema.Factorders
```

### Upsert / Merge

If the Fact Orders table already exists, Delta Lake `MERGE` is used.

The merge condition is based on:

```text
order_id
DimCustomerKey
DimProductKey
```

Matched records are updated, while unmatched records are inserted.

This makes the Fact Orders load **incremental/upsert-oriented** rather than blindly recreating the table every time.

Source: `Gold Orders.py`

---

# 🔄 Parameterization and Iterative Processing

Notebook: `Parameters.py`

The project defines the datasets that need to be processed:

```python
datasets = [
    {"file_name": "orders"},
    {"file_name": "products"},
    {"file_name": "customers"}
]
```

These dataset names are stored using Databricks task values.

This supports a reusable ingestion workflow instead of creating a completely separate Bronze ingestion notebook for each dataset.

Source: `Parameters.py`

---

# 🧱 Storage Structure

The project uses Azure Data Lake Storage Gen2 containers/folders for the different layers.

Conceptually:

```text
Azure Data Lake Storage
│
├── source/
│   ├── customers
│   ├── orders
│   └── products
│
├── bronze/
│   ├── customers
│   ├── orders
│   ├── products
│   └── checkpoints
│
├── silver/
│   ├── customers
│   ├── orders
│   ├── products
│   └── regions
│
└── gold/
    ├── DimCustomers
    ├── DimProducts
    └── Factorders
```

---

# 🗃️ Databricks Catalog Structure

The project uses the `project_cata` catalog.

Main objects include:

```text
project_cata
│
├── bronze
│   └── regions
│
├── silver
│   ├── customers_silver
│   ├── orders_silver
│   ├── products_silver
│   └── regions_silver
│
├── gold_schema
│   ├── DimCustomers
│   └── Factorders
│
└── default
    └── DimProducts
```

> The exact catalog/schema names follow the notebooks supplied with this project.

---

# 🛠️ Technologies Used

- **Microsoft Azure**
- **Azure Data Lake Storage Gen2**
- **Azure Blob/ADLS storage**
- **Azure Databricks**
- **Apache Spark**
- **PySpark**
- **Spark Structured Streaming**
- **Databricks Auto Loader**
- **Delta Lake**
- **Delta Tables**
- **Delta Live Tables (DLT)**
- **Databricks SQL**
- **Python**
- **GitHub**

---

# 🔑 Key Data Engineering Concepts Demonstrated

This project demonstrates several important real-world data engineering concepts:

### 1. Medallion Architecture

```text
Bronze → Silver → Gold
```

- Bronze = ingestion/raw layer
- Silver = cleaned/transformed layer
- Gold = business-ready layer

### 2. Incremental Data Ingestion

Auto Loader and Structured Streaming are used for ingestion, with checkpoint locations to maintain streaming state.

### 3. Delta Lake

Silver and Gold datasets are stored using Delta format.

### 4. Data Quality

DLT expectations are used for product data:

```text
product_id IS NOT NULL
product_name IS NOT NULL
```

### 5. Surrogate Keys

The customer dimension generates a `DimCustomerKey`.

### 6. Slowly Changing Dimensions

The project demonstrates:

```text
DimCustomers → SCD Type 1
DimProducts  → SCD Type 2
```

### 7. Delta MERGE / Upsert

Fact Orders and Dim Customers use Delta merge logic to update existing records and insert new records.

### 8. Window Functions

The Orders transformation demonstrates:

- `dense_rank`
- `rank`
- `row_number`

### 9. SQL UDF

The Products transformation creates and uses a SQL user-defined function for calculating discounted price.

### 10. Parameterized Processing

The Bronze ingestion notebook accepts a dataset/file name through a Databricks widget.

---

# 🚀 Project Execution Flow

A typical execution sequence is:

```text
1. Upload / place source Parquet files
              ↓
2. Run Parameters
              ↓
3. Bronze Auto Loader
              ↓
4. Create Bronze data
              ↓
5. Run Silver Customers
   Run Silver Orders
   Run Silver Products
   Run Silver Regions
              ↓
6. Create/register Silver Delta tables
              ↓
7. Run Gold Customers
              ↓
8. Run Gold Products DLT pipeline
              ↓
9. Run Gold Orders
              ↓
10. Query Gold dimensions and Fact Orders
```

# 📊 Final Data Model

The Gold layer can be viewed conceptually as:

                  ┌──────────────────┐
                  │   DimCustomers   │
                  │                  │
                  │ DimCustomerKey   │
                  │ customer_id      │
                  └────────┬─────────┘
                           │
                           │
                           ▼
                    ┌──────────────┐
                    │  FactOrders  │
                    │              │
                    │ order_id     │
                    │ customer key │
                    │ product key  │
                    │ order data   │
                    └──────┬───────┘
                           │
                           │
                           ▼
                  ┌──────────────────┐
                  │   DimProducts    │
                  │                  │
                  │ DimProductKey    │
                  │ product_id       │
                  │ product details  │
                  └──────────────────┘
```

This creates a simple **star-schema-style analytical model**, with dimensions surrounding the Fact Orders table.

---

# 🎯 Project Objective

The primary objective of this project is to build a scalable cloud data pipeline that:

- Ingests Parquet data into Azure Data Lake
- Uses Databricks for distributed data processing
- Cleans and transforms raw data
- Stores curated data in Delta format
- Implements dimensional modeling
- Demonstrates SCD Type 1 and SCD Type 2
- Uses Delta Lake MERGE for upserts
- Implements data quality expectations
- Creates a business-ready Fact table
- Demonstrates parameterized and reusable ingestion

---

## ⚠️ Notes

This README describes the implementation represented by the supplied Databricks notebooks and architecture screenshots. Some notebook code is demonstration/project code and may require environment-specific changes before execution, such as Azure storage account/container names, catalog/schema names, permissions, and Databricks pipeline configuration.
