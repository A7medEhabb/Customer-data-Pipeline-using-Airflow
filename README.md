# Customer Data Pipeline using Apache Airflow

## 📌 Summary
This project implements a **Customer Data Pipeline** using **Apache Airflow**. The pipeline automatically generates synthetic customer, product, store, and transaction data, calculates key business metrics, sends daily reports via email, and cleans up generated files. The goal is to demonstrate a complete automated workflow from data generation to reporting.

---

## 🖼 Pipeline Visualization
![Pipeline Diagram](<img width="801" height="381" alt="Untitled Diagram drawio" src="https://github.com/user-attachments/assets/ea67e160-419c-45a5-bd3e-8546818d6b6d" />
)
*Replace `path_to_your_uploaded_image.png` with the actual image file name.*

---

## 🧩 Project Components & Their Use

| Component | Purpose |
|-----------|---------|
| **create_tables** | Creates or resets PostgreSQL tables for customers, products, stores, and transactions. |
| **generate_customers_data** | Generates synthetic customer data. |
| **generate_product_data** | Generates synthetic product data. |
| **generate_stores_data** | Generates synthetic store data. |
| **generate_transaction_data** | Generates synthetic transaction records linking customers, products, and stores. |
| **calculate_metrics** | Computes business KPIs from generated transaction data. |
| **send_email** | Sends a daily report via email (using SendGrid). |
| **delete_files** | Deletes all generated CSV files and reports to maintain a clean environment. |

---

## 🔄 Data Pipeline Overview
The pipeline is defined as a **Directed Acyclic Graph (DAG)** in Airflow with the following workflow:

1. **Schema Initialization**: Reset PostgreSQL tables.  
2. **Data Generation**: Parallel creation of customers, products, stores, and transaction data.  
3. **Metrics Calculation**: Aggregation and computation of key business metrics.  
4. **Reporting**: Generate daily report and send via email.  
5. **Cleanup**: Remove all temporary files to ensure ephemeral storage.  

This DAG runs **daily**, ensures dependencies between tasks, allows parallel execution for data generation, and automates the entire workflow from data creation to reporting.

---

*For more details, see the DAG code in `dags/pipeline.py`.*
