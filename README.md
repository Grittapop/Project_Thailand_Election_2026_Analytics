# 🇹🇭 Thailand Election 2026 Analytics  
## Modern Data Lakehouse Architecture

---

## 📌 Project Overview

This project implements a **modern Data Lakehouse architecture** for analyzing Thailand Election 2026 data.

The system ingests data from multiple APIs, processes it using a **Medallion Architecture (Bronze / Silver / Gold)**, and serves curated datasets to **Power BI** via **Trino**.

The entire platform is fully containerized using **Docker** and orchestrated with **Dagster (OSS)**.

---

## 🏗 Architecture Overview


Containerized deployment

🏗 System Architecture

![Screenshot 2026-02-26 023213](https://github.com/user-attachments/assets/5d212b15-03a0-4d64-9fe7-9814505894f0)


---

## 🧱 Data Sources

The platform ingests election data from 7 APIs:

- `province`
- `constituency`
- `party`
- `mp_candidate`
- `party_candidate`
- `stats_cons`
- `stats_party`

Data source reference:  
Facebook public dataset used for election reporting.

---

## ⚙️ Orchestration Layer – Dagster (OSS)

Dagster is used to:

- Orchestrate ingestion pipelines
- Manage task dependencies
- Schedule workflows
- Monitor pipeline execution
- Handle retries and logging

![Screenshot 2026-02-26 023241](https://github.com/user-attachments/assets/76015b74-d9de-428f-bf9e-54b6586e6a6e)


![Screenshot 2026-02-26 023327](https://github.com/user-attachments/assets/ab6b6552-6017-473b-a868-1192e6f1404c)


---

## 🪣 Storage Layer – MinIO (S3-Compatible Object Storage)

MinIO serves as the Data Lake storage layer.

### 🟤 Bronze Layer (Raw Zone)

- Stores raw JSON from APIs
- No transformation applied
- Ensures reproducibility and traceability

### ⚪ Silver Layer (Cleaned Zone)

- Transforms JSON into structured tables
- Stored in **Parquet format**
- Managed using **Apache Iceberg**
- Supports schema evolution and versioning

### 🟡 Gold Layer (Curated BI Zone)

- Built using **dbt**
- Includes:
  - Fact tables
  - Dimension tables
- Stored in Parquet + Iceberg format
- Optimized for analytics workloads

---

## 🧊 Apache Iceberg Integration

Apache Iceberg is used as the table format to enable:

- ACID transactions on Data Lake
- Snapshot management
- Time travel queries
- Schema evolution
- Partition pruning

Iceberg catalog is backed by PostgreSQL.

---

## 🐘 Metadata Layer – PostgreSQL

PostgreSQL is used as the Iceberg metadata catalog to store:

- Table metadata
- Snapshot history
- Version information
- Schema definitions

This allows Trino to resolve the latest table state.

---

## 🔍 Query Engine – Trino

Trino is used as a distributed SQL query engine with the Iceberg connector.

Trino is configured to connect to:

- Apache Iceberg tables
- MinIO object storage
- PostgreSQL-backed Iceberg catalog

### 🖥 SQL Client – DBeaver

DBeaver is used as the SQL client to:

- Connect to Trino
- Explore schemas and tables
- Execute analytical queries
- Inspect Iceberg tables
- Validate transformations in Silver and Gold layers

![Screenshot 2026-02-26 023617](https://github.com/user-attachments/assets/a99c9f40-5ea5-411d-b68c-6e6c9a0f2c48)

---

## 🔌 BI Integration – ODBC Driver

Power BI connects to Trino using the **InsightSoftware Trino ODBC Driver**:

https://insightsoftware.com/drivers/trino-odbc-jdbc/

This driver enables Power BI to execute SQL queries against Trino, which in turn reads curated Iceberg tables from the Gold layer.

---

## 🐳 Containerization

All services are fully containerized using Docker:

- Dagster (Orchestration)
- MinIO (Object Storage)
- PostgreSQL (Iceberg Catalog)
- Trino (Query Engine)
- dbt (Transformation Layer)

### Benefits

- Environment consistency across development and production
- Reproducible infrastructure
- Service isolation
- Scalable architecture
- Simplified deployment using Docker Compose

---

## 📊 Analytics & Business Intelligence

The final layer of this project focuses on transforming curated data from the Gold layer into meaningful analytical insights using Power BI.

The dashboards are designed to support executive reporting, party-level comparison, and geographic performance analysis. Each report provides a different analytical perspective of the Thailand General Election 2026.

---

## 🏛 Executive Overview


![Screenshot 2026-02-26 032406](https://github.com/user-attachments/assets/53da46f7-95a0-4db4-9f67-7e047fd4d0ba)


The Executive Overview dashboard provides a national-level summary of the election results.

This page is designed for decision-makers who need a clear and concise understanding of overall performance before diving into detailed analysis.

It presents key indicators such as total registered voters, turnout, total valid votes, and the leading political party. These metrics provide a quick snapshot of participation levels and overall electoral outcomes.

The visualizations on this page allow users to:

- Understand national turnout patterns  
- Compare total votes across political parties  
- Identify the most competitive constituencies  
- Observe high-level geographic distribution of voter engagement  

The goal of this page is clarity, speed, and strategic insight.


---

## 🗺 Geographic Analysis (Province & Constituency Level)


![Screenshot 2026-02-26 032432](https://github.com/user-attachments/assets/c840b5fb-87bc-45e7-8dc6-88816d1d1493)


The Geographic Analysis report provides a regional breakdown of election performance.

This page focuses on spatial insights by examining:

- Leading party by province  
- Total votes by province  
- Turnout rates by constituency  
- Ranking of constituencies based on total votes  

The map visualization highlights regional variation in turnout and party dominance, making it easy to identify geographic trends.

This level of analysis supports:

- Regional strategy evaluation  
- Identification of high-performance districts  
- Comparison between urban and rural voting behavior  
- Detection of areas with strong or weak electoral engagement  

The geographic layer transforms national data into localized insights.

---

## 🗳 Party-Level Analysis


![Screenshot 2026-02-26 032500](https://github.com/user-attachments/assets/ce1365ef-09fb-435b-bfc8-19e0e590eb57)


The Party-Level Analysis report focuses on comparing political party performance across the country.

This section evaluates how each party performed in terms of seats won, vote share, and competitiveness.

It enables users to:

- Compare party dominance  
- Analyze vote efficiency  
- Identify stronghold regions  
- Detect closely contested races  

The dashboard supports filtering by party, allowing deeper inspection of performance patterns and competitive margins.

This view is especially useful for evaluating strategic positioning and electoral competitiveness between major and minor parties.
---

## 🧱 Analytical Data Model

![Screenshot 2026-02-26 025957](https://github.com/user-attachments/assets/60cd2a00-ea07-4384-8287-9464df1ccf0f)


The dashboards are built on a structured analytical data model using fact and dimension tables.

Dimension tables provide contextual information such as province, constituency, party, and candidate details.  
Fact tables store measurable election results such as vote counts, turnout, and rankings.

This structured modeling approach ensures:

- Clear separation between descriptive data and measurable metrics  
- Efficient querying and filtering  
- Scalable expansion for additional analysis  
- Consistent performance across reports  

The model supports multi-level drill-down from national to provincial and constituency views.

---
## 📚 References & Documentation

This project is built using modern open-source data tools.  
Below are the official documentation and source references used in this project.

### 🟣 Dagster (Orchestration)

Dagster is used to orchestrate all data pipelines.

Official Deployment Guide (Docker):
https://docs.dagster.io/deployment/oss/deployment-options/docker

---

### 🧊 Trino (Distributed SQL Engine)

Trino is used as the distributed query engine to query Iceberg tables.

Iceberg Connector Documentation:
https://trino.io/docs/current/connector/iceberg.html

---

### 🧊 Apache Iceberg (Table Format)

Iceberg is used as the table format for managing ACID transactions,
schema evolution, and snapshot-based versioning on the Data Lake.

Official Website:
https://iceberg.apache.org/

---

### 🔌 Trino ODBC Driver (Power BI Integration)

Power BI connects to Trino using the InsightSoftware ODBC driver.

Official Driver Page:
https://insightsoftware.com/drivers/trino-odbc-jdbc/

---

### 📊 Data Source

Election data is sourced from publicly available reporting published via Facebook.

Original Source:
https://www.facebook.com/story.php?story_fbid=10166018100020348&id=757045347

---

### 🐘 PostgreSQL (Iceberg Catalog Backend)

PostgreSQL is used as the Iceberg metadata catalog backend.

Official Website:
https://www.postgresql.org/

---

### 🪣 MinIO (Object Storage)

MinIO is used as S3-compatible object storage for the Data Lake.

Official Website:
https://min.io/

---

# 👤 Author

**Grittapop**  
Data Engineer | Analytics Engineer | Data Analyst  
Portfolio Project

