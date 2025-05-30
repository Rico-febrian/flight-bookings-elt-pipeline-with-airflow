# 🏁 Getting Started: Run This Project

**Welcome to the Learning Logs!** 

This project is a hands-on example of an ELT (Extract, Load, Transform) data pipeline using **Apache Airflow** as the orchestrator.

---

## 🧠 Project Overview

This project demonstrates how to:

* **Extract** data from a source (PostgreSQL) database
* **Load** it into a staging schema in a data warehouse (also PostgreSQL) and
* **Transform** it into a final schema for analysis or reporting

---

## 🔄 How the Pipeline Works

![elt-design](https://github.com/Rico-febrian/elt-pipeline-with-airflow-for-travel-business/blob/main/pict/elt-design.png)

* **Extract Task**: Reads data from source DB and stores it as CSV in MinIO
* **Load Task**: Loads CSVs from MinIO to staging schema in warehouse
* **Transform Task**: Executes SQL to transform staging data into final schema

---

## 🎯 Objectives

By completing this project, you will:

* Understand the basic structure of an ELT pipeline
* Learn how to use Apache Airflow (Standalone) to manage DAG, connection and pipeline.
* Run and monitor DAGs through the Airflow UI

---

## ⚙️ Requirements

Make sure you have the following tools installed:

- Docker or Docker Desktop with WSL enabled
- Python 3.7 or newer
- DBeaver (or any PostgreSQL-compatible database client)

---

## 📁 Project Structure

```
elt-airflow-project/
├── dags/
│   └── travel_elt_pipeline/
│       ├── helper/                  # Helper functions
│       ├── query/                   # SQL file to extract each table from data source
│       ├── transformation_models/   # SQL file to transform data from staging to final schema
│       ├── tasks/                   # Core task scripts (extract, load, transform)
│       └── run.py                   # Main DAG definition
├── database_schema/
│   ├── source/                      # Source database init SQL
│   └── warehouse/                   # Warehouse schema init SQL (staging and final schema)
├── Dockerfile                       # Custom Airflow Standalone Dockerfile
├── docker-compose.yml               # Docker Compose config
├── fernet.py                        # Fernet key generator
├── start.sh                         # Entrypoint script
├── requirements.txt                 # Python packages for Airflow
└── README.md                        # This guide
```

---

## 🚀 Getting Started

### 1. Clone the Repository

```bash
git clone https://github.com/your-username/elt-airflow-project.git
cd elt-airflow-project
```

### 2. Generate Fernet Key

This key encrypts credentials in Airflow connections.

```bash
pip install cryptography==45.0.2
python3 fernet.py
```

Copy the output key to the `.env` file.

### 3. Create `.env` File

Use the following template and update with your actual configuration:

```ini
# Fernet key for encrypting Airflow connections (generate using fernet.py script)
AIRFLOW_FERNET_KEY=...

# Airflow metadata database connection URI
AIRFLOW_DB_URI=postgresql+psycopg2://<AIRFLOW_DB_USER>:<AIRFLOW_DB_PASSWORD>@<AIRFLOW_METADATA_CONTAINER_NAME>:<AIRFLOW_DB_PORT>/<AIRFLOW_DB_NAME>
# eg: postgresql+psycopg2://airflow:airflow@airflow_metadata:5433/airflow

# Airflow DB configuration
AIRFLOW_DB_USER=...           # eg: airflow
AIRFLOW_DB_PASSWORD=...       # eg: airflow
AIRFLOW_DB_NAME=...           # eg: airflow
AIRFLOW_DB_PORT=...           # eg: 5433 (default PostgreSQL is 5432, but you can use 5433+ to avoid conflicts)
AIRFLOW_PORT=...              # eg: 8080 (common ports: 8080, 8081, etc.)

# Source database configuration
SOURCE_DB_HOST=...            # eg: localhost
SOURCE_DB_NAME=...            # eg: sources
SOURCE_DB_USER=...            # eg: postgres
SOURCE_DB_PASSWORD=...        # eg: postgres123
SOURCE_DB_PORT=...            # eg: 5434 (default PostgreSQL is 5432, but you can use 5433+ to avoid conflicts)

# Data warehouse (DWH) configuration
DWH_DB_HOST=...               # eg: localhost
DWH_DB_NAME=...               # eg: warehouse
DWH_DB_USER=...               # eg: postgres
DWH_DB_PASSWORD=...           # eg: postgres123
DWH_DB_PORT=...               # eg: (default PostgreSQL is 5432, but you can use 5433+ to avoid conflicts)

# MinIO configuration
MINIO_ROOT_USER=...           # eg: minioadmin
MINIO_ROOT_PASSWORD=...       # eg: minio123
MINIO_API_PORT=...            # eg: 9000 (commonly used for MinIO API)
MINIO_CONSOLE_PORT=...        # eg: 9001 (commonly used for MinIO Console)
```
### 4. Extract Source Dataset
```bash
cd database_schema/source/
unzip init.zip
```

### 5. Build and Start Services

```bash
docker-compose up --build -d
```

### 6. Create Airflow User

```bash
docker exec -it airflow_standalones airflow users create \
    --username your_username \
    --firstname First \
    --lastname Last \
    --role Admin \
    --email your@email.com \
    --password your_password
```

### 7. Open Airflow UI

Access the UI at: [http://localhost:8080](http://localhost:8080) (or your defined port).

Log in with the credentials you created.

---

## 🔌 Setup Airflow Connections

You need to create 3 connections via the Airflow UI:

* **PostgreSQL - Source Database**
* **PostgreSQL - Data Warehouse**
* **MinIO - Object Storage**

Steps:

1. Go to **Admin > Connections**
2. Click **+** to add new connection
3. Fill in the details as per your `.env` setup

### Source and Warehouse Connection

![database-connection](https://github.com/Rico-febrian/elt-pipeline-with-airflow-for-travel-business/blob/main/pict/database-connection.png)

### MinIO Connection

![minio-connection](https://github.com/Rico-febrian/elt-pipeline-with-airflow-for-travel-business/blob/main/pict/minio-connection.png)

---

## ▶️ Run the DAG

1. In the Airflow UI, find the DAG named `travel_elt_pipeline`
2. Click the **Play** button to trigger it
3. Monitor task progress via the **Graph View** or **Tree View**

---

## ✅ Verify the Results

- ### Extracted Data in MinIO Bucket
  - Log in to the MinIO console (eg. localhost:9000) using the username and password defined in your `.env` file.
  - Navigate to the selected bucket.
  - You should see the extracted data files in CSV format.
    
    ![extracted-data](https://github.com/Rico-febrian/elt-pipeline-with-airflow-for-travel-business/blob/main/pict/extracted-file-in-minio.jpg)

- ### Staging and Transformed data in Data Warehouse
  To verify the data in your data warehouse:

  - Open your preferred database client (e.g., DBeaver).
  - Connect to your warehouse database.
  - Check the following:
      - ✅ Raw data from the source should be available under the **staging** schema.
      - ✅ Transformed data should be available under the **final** schema.

- ### DAG Result
    
    - DAG Graph

      ![dag-graph](https://github.com/Rico-febrian/elt-pipeline-with-airflow-for-travel-business/blob/main/pict/dag%20graph.png)

    - Extract Task (running in parallel)

      ![extract-task](https://github.com/Rico-febrian/elt-pipeline-with-airflow-for-travel-business/blob/main/pict/extract-task.png)

    - Load Task (running sequentially)

      ![load-task](https://github.com/Rico-febrian/elt-pipeline-with-airflow-for-travel-business/blob/main/pict/load-task.png)

    - Transform Task (running sequentially)

      ![transform-task](https://github.com/Rico-febrian/elt-pipeline-with-airflow-for-travel-business/blob/main/pict/transform-task.png)

---

## 📬 Feedback & Articles

**Thank you for exploring this project!** If you have any feedback, feel free to share, I'm always open to suggestions.

Additionally, I write about my learning journey on Medium. You can check out my articles [here](https://medium.com/@ricofebrian731). Let’s also connect on [LinkedIn](https://www.linkedin.com/in/ricofebrian).

---

Happy learning! 🚀
