# Customer Data Pipeline using Apache Airflow

## 📌 Summary
This project implements a **Customer Data Pipeline** using **Apache Airflow**. The pipeline automatically generates synthetic customer, product, store, and transaction data, calculates key business metrics, sends daily reports via email, and cleans up generated files. The goal is to demonstrate a complete automated workflow from data generation to reporting.

---

## 🖼 Pipeline Visualization
<img width="801" height="381" alt="Untitled Diagram drawio" src="https://github.com/user-attachments/assets/ea67e160-419c-45a5-bd3e-8546818d6b6d" />



---

## 🧩 Project Components & Their Use

| Component | Purpose |
|-----------|---------|
| **create_tables** | Creates or resets PostgreSQL tables for customers, products, stores, and transactions. |
| **generate_customers_data** | Generates synthetic customer data using Faker. |
| **generate_product_data** | Generates synthetic product data using Faker. |
| **generate_stores_data** | Generates synthetic store data using Faker. |
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

<img width="1920" height="1080" alt="Screenshot from 2025-12-04 11-04-47" src="https://github.com/user-attachments/assets/396997bb-f63d-46b3-a98f-38538d824fd9" />


<img width="1920" height="1080" alt="Screenshot from 2025-12-04 11-05-38" src="https://github.com/user-attachments/assets/cb7cc7f0-a3e0-4bb4-93c3-da93412d85d9" />

---

*For more details, see the DAG code in `dags/pipeline.py`.*

[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/A7medEhabb/Customer-data-Pipeline-using-Airflow)
