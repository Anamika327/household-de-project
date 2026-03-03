# Household Spend & Grocery Analytics – Data Engineering Project

## Problem Statement

This project builds a layered data pipeline to process nested transaction data 
for household spending and grocery inventory tracking.

The pipeline ingests raw nested JSON data, applies schema enforcement and 
data quality validation, transforms it into a star schema model, and 
prepares it for analytics dashboards.

## Architecture

Source (Nested JSON)
        ↓
Bronze Layer (Raw Delta Tables)
        ↓
Silver Layer (Flattened + Validated)
        ↓
Star Schema (Fact + Dimensions)

## Tech Stack

- PySpark (ETL processing)
- Delta Lake (ACID storage + schema enforcement)
- Git (commit-driven development workflow)
- Delta Lake
- Git (commit-based development)
- Star Schema Modeling
- Data Quality & Quarantine Design

## Business Processes Modeled

This warehouse models three independent business processes:

1. Expense Tracking (Purchases)
2. Consumption Tracking (Usage of Items)
3. Inventory Snapshot Tracking

Each process has its own fact table but shares conformed dimensions 
(dim_date, dim_item, dim_member, dim_store).


## Data Model

The Silver layer is modeled using a star schema with three fact tables:

- fact_expenses
- fact_consumption
- fact_inventory_snap

Conformed dimensions:
- dim_date
- dim_item
- dim_category
- dim_member
- dim_store


## Layer Responsibilities

- Bronze Layer: Stores raw source data in original nested format (Delta tables).
- Silver Layer: Applies data quality validation, flattens nested structures, and models data into fact and dimension tables.
- Gold Layer (future): Aggregated metrics for dashboard consumption.

## Fact Table Grain

- fact_expenses: One row per item purchased per transaction.
- fact_consumption: One row per item consumed per member per day.
- fact_inventory_snap: One row per item per day snapshot.



                         ┌───────────────────┐
                         │     dim_date      │
                         │-------------------│
                         │ date_key (PK)     │
                         │ full_date         │
                         │ day               │
                         │ month             │
                         │ quarter           │
                         │ year              │
                         └─────────┬─────────┘
                                   │
                                   │
         ┌─────────────────────────┼─────────────────────────┐
         │                         │                         │
         ▼                         ▼                         ▼

┌───────────────────┐     ┌───────────────────┐     ┌──────────────────────┐
│   fact_expenses   │     │ fact_consumption  │     │ fact_inventory_snap  │
│-------------------│     │-------------------│     │----------------------│
│ transaction_id    |
|(degenerate dimension)
| expense_key (PK)  │     │ consumption_key   │     │ snapshot_key (PK)    │
│ date_key (FK)     │     │ date_key (FK)     │     │ date_key (FK)        │
│ category_key (FK) │     │ item_key (FK)     │     │ item_key (FK)        │
│ member_key (FK)   │     │ member_key (FK)   │     │ current_stock        │
│ store_key (FK)    │     │ quantity_used     │     │ reorder_level        │
│ amount            │     └──────────┬────────┘     └──────────┬───────────┘
│ payment_method    │                │                          │
└─────────┬─────────┘                │                          │
          │                          │                          │
          │                          │                          │
          ▼                          ▼                          ▼

┌───────────────────┐     ┌───────────────────┐     ┌───────────────────┐
│   dim_category    │     │     dim_item      │     │    dim_member     │
│-------------------│     │-------------------│     │-------------------│
│ category_key (PK) │     │ item_key (PK)     │     │ member_key (PK)   │
│ category_name     │     │ item_name         │     │ member_name       │
│ category_group    │     │ category_key (FK) │     │ role              │
│ is_grocery        │     │ unit              │     │ active_flag       │
└───────────────────┘     │ perishable_flag   │     └───────────────────┘
                          │ shelf_life_days   │
                          └─────────┬─────────┘
                                    │
                                    ▼
                           ┌───────────────────┐
                           │    dim_store      │
                           │-------------------│
                           │ store_key (PK)    │
                           │ store_name        │
                           │ store_type        │
                           │ city              │
                           └───────────────────┘


## How to Run

1. Install dependencies:
   pip install -r requirements.txt

2. Run Bronze ingestion:
   python src/bronze/ingest_transactions.py

3. Run Silver transformation:
   python src/silver/transform_transactions.py


## Key Concepts Demonstrated

- Nested schema handling (StructType, ArrayType)
- Schema enforcement during ingestion
- Delta Lake storage format
- Data quality validation and quarantine pattern
- Star schema modeling
- Surrogate key generation
- Layered (Bronze/Silver) architecture

## Current Implementation Scope

The initial version implements the Purchase (Expense) process end-to-end. 
Consumption and Inventory processes will be added incrementally.

## Future Enhancements

- Implement incremental processing using Delta MERGE
- Add data quality monitoring metrics
- Introduce Gold layer with aggregated KPI tables
- Deploy pipeline using Airflow or Databricks Workflows
- Load curated data into Snowflake for BI consumption