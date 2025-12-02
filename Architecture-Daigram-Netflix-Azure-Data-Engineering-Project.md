          ┌─────────────────────────────┐
          │   GitHub Public Repository   │
          │     (Netflix CSV Files)      │
          └──────────────┬───────────────┘
                         │
                         ▼
          ┌─────────────────────────────┐
          │  Azure Data Factory (ADF)    │
          │ Web Activity, ForEach, Copy  │
          └──────────────┬───────────────┘
                         │ Raw Ingestion
                         ▼
        ┌────────────────────────────────────┐
        │      ADLS Gen2 - Bronze Layer       │
        │ Raw files stored as CSV             │
        └────────────────┬───────────────────┘
                         │ Auto Loader
                         ▼
        ┌────────────────────────────────────┐
        │        Azure Databricks             │
        │ Auto Loader + PySpark Transform     │
        │ Widgets + Workflows + Notebooks     │
        └──────────────┬─────────────────────┘
                        │ Silver Layer Writes
                        ▼
        ┌────────────────────────────────────┐
        │     ADLS Gen2 - Silver Layer        │
        │ Delta Tables (cleansed/refined)     │
        └────────────────┬───────────────────┘
                         │ DLT Pipeline
                         ▼
        ┌────────────────────────────────────┐
        │   Delta Live Tables (DLT)           │
        │ Data Quality Rules + Expectations   │
        │ Gold Tables Creation                │
        └────────────────┬───────────────────┘
                         │
                         ▼
        ┌────────────────────────────────────┐
        │     ADLS Gen2 - Gold Layer          │
        │ Analytics Ready Curated Data         │
        └────────────────────────────────────┘
