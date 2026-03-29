# Spark — PySpark practice

Small **Apache Spark / PySpark** exercises, mostly written for **Databricks**. Two separate projects live in this repo.

---

## 1. IPL data analysis (`IPL_DATA_Analysis_SPARK/`)

**Cricket (IPL)** analytics on ball-by-ball, match, player, and team data.

- **Entry point:** `IPL_DATA_Analysis_SPARK/IPL_DATA_ANALYSIS_SPARK.py` (exported Databricks notebook).
- **Stack:** PySpark DataFrames, SQL (`%sql` / temp views), window functions, matplotlib/seaborn for charts.
- **Data:** CSVs are read from **`s3://tejas-dev-bucket/`** (update paths if you use another bucket or local files).
- **Run:** Intended for **Databricks** (uses `display()`, notebook cells, `global_temp` views). For vanilla Spark, strip Databricks-only calls and point reads to your data paths.

---

## 2. Brazilian e-commerce — Olist (`Project/`)

**Retail** exploration on the public **Olist** datasets (orders, customers, products, sellers, reviews, etc.).

- **Entry point:** `Project/spark_main.ipynb` (Databricks notebook metadata).
- **Stack:** PySpark session, CSV ingestion, schema inspection, row counts, sample previews; optional Python `logging` to `spark.log`.
- **Data:** Sample CSVs are under **`Project/Input/`**. The notebook loads from a Databricks **Unity Catalog volume** path (`/Volumes/workspace/pyspark/retail-sales/...`); change those paths to match your workspace or use local `Input/` paths for local runs.
- **Config:** `Project/log4j2.properties` is a Databricks-oriented Spark logging template.

---

## Requirements

- **PySpark** (version aligned with your Spark/Databricks runtime).
- **Databricks** recommended for notebooks as written; **matplotlib** and **seaborn** are used in the IPL script.
