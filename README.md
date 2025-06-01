# 🏁 Getting Started: Run This Project

**Welcome to the Learning Logs!** 

This project is a hands-on example of an ELT (Extract, Load, Transform) data pipeline using **Apache Airflow** as the orchestrator.

---

## 🧠 Project Overview

This project demonstrates how to:

- **Extract** data from a source database
- **Load** it into a staging schema in a data warehouse
- **Transform** it into a final schema for analysis or reporting and
- **Orchestrate** the entire ELT process using Airflow, including:

  - Managing connections and variables
  - Creating dynamic tasks and
  - Handling task failures and errors in Airflow
    
---

## 🔄 How the Pipeline Works

![elt-design](https://github.com/Rico-febrian/elt-pipeline-with-airflow-for-travel-business/blob/main/pict/elt-design.png)

* **Extract Task**: Reads data from source DB and stores it as CSV in MinIO
* **Load Task**: Loads CSVs from MinIO to staging schema in warehouse
* **Transform Task**: Executes SQL to transform staging data into final schema

---

## 🎯 Objectives

By completing this project, you will:

- Understand the basic structure of an ELT pipeline

- Learn how to use Apache Airflow (Standalone) to:
    - Manage connection and variables
    - Create dynamic task and
    - Handle task failures and errors

- Run and monitor DAGs through the Airflow UI

---

## ⚙️ Requirements

Make sure your setup meets these minimum requirements:

- A laptop with at least **8GB of RAM**

  (Note: 8GB is the bare minimum, especially if you’re running on Windows. If possible, go for 16GB for a smoother experience.)

- Docker or Docker Desktop with WSL enabled

- Python 3.7 or newer

- DBeaver (or any PostgreSQL-compatible database client)

---

## 📁 Project Structure

```
elt-airflow-project/
├── dags/
│   └── flight_elt_pipeline/
│       ├── helper/                  # Helper functions
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

You need to create **5 connections** via the **Airflow UI**:

1[connections-list]()

- `sources-conn` and `warehouse-conn`
    
    - **Type**: PostgreSQL
    - **Description**: Connection to source and warehouse database

      ![db-conn]()

- `minio-conn`
    
    - **Type**: Amazon Web Service
    - **Description**: Connection to MinIO (used as object storage/data lake)

      ![minio-conn]()

- `slack_notifier`

    - **Type**: HTTP
    - **Description**: Connection to a Slack channel for error alerts
    - **Setup Steps**:

      1. **Log in** to your existing Slack account or **create** a new one if you don’t have it yet.
      2. **Create a workspace** (if you don’t already have one) and create a dedicated Slack channel where you want to receive alerts.
      3. **Create a Slack App**:

         - Go to https://api.slack.com/apps
         - Click **Create New App**
         - Choose **From scratch**
         - Enter your app name and select the workspace you just created or want to use
         - Click **Create App**
    
      4. **Set up an Incoming Webhook** for your app:

         - In the app settings, find and click on **Incoming Webhooks**
         - **Enable Incoming Webhooks** if it’s not already enabled
         - Click **Add New Webhook to Workspace**
         - Select the Slack channel you want alerts to go to and authorize
    
      5. **Copy the generated Webhook URL**
      6. In your Airflow UI, create a new connection called `slack_notifier` with:

         - **Connection Type**: HTTP
         - **Password field**: paste the copied Webhook URL here

         ![slack-conn]()
          
---

## 🔌 Setup Airflow Variables

You need to create **4 variables** via the **Airflow UI**:

![variables-list]()

- `incremental`
    
    - **Value**: `True` or `False`. Default value is `True`
    - **Description**:
        
        - If set to `True`, the pipeline will perform **incremental** Extract and Load processes, meaning it only processes new or updated data based on the created_at or updated_at columns in each table.
        
        - If set to `False`, the pipeline will run a **full load**, extracting and loading all data every time, regardless of changes.

- `tables_to_extract`
    
    - **Value**:

      ```bash
      ['aircrafts_data', 'airports_data', 'bookings', 'tickets', 'seats', 'flights', 'ticket_flights', 'boarding_passes']
      ```
    
    - **Description**: This variable will be used to create dynamic tasks during the data extraction process.

- `tables_to_load`

    - **Value**:

      ``` bash
      {
          "aircrafts_data": "aircraft_code",
          "airports_data": "airport_code",
          "bookings": "book_ref",
          "tickets": "ticket_no",
          "seats": ["aircraft_code", "seat_no"],
          "flights": "flight_id",
          "ticket_flights": ["ticket_no", "flight_id"],
          "boarding_passes": ["ticket_no", "flight_id"]
      }
      ```

    - **Description**: This variable will be used to create dynamic tasks during the data loading process to the staging area. The keys of the dictionary represent the table names, and the values represent the primary keys of those tables.

- `tables_to_transform`
    
    - **Values**:

      ```bash
      [
        "dim_aircraft",
        "dim_airport",
        "dim_passenger",
        "dim_seat",
        "fct_boarding_pass",
        "fct_booking_ticket",
        "fct_flight_activity",
        "fct_seat_occupied_daily"
      ]
      ```
      
    - **Description**: This table will be used to create dynamic tasks during the data transformation process.

---

## ▶️ Run the DAG

1. In the Airflow UI, find the DAG named `flight_elt_pipeline`
2. Click the **Play** button to trigger it
3. Monitor task progress via the **Graph View** or **Tree View**

---

## ✅ Verify the Results

- ### Extracted Data in MinIO Bucket
  - Log in to the MinIO console (eg. localhost:9000) using the username and password defined in your `.env` file.
  - Navigate to the selected bucket.
  - You should see the extracted data files in CSV format.
    
    ![extracted-data]()

- ### Staging and Transformed data in Data Warehouse
  To verify the data in your data warehouse:

  - Open your preferred database client (e.g., DBeaver).
  - Connect to your warehouse database.
  - Check the following:
    
      - ✅ Raw data from the source should be available under the **staging** schema.
      - ✅ Transformed data should be available under the **final** schema.

- ### DAG Result

  Since incremental mode and catchup are enabled (set to `True`), the pipeline runs **starting from the `start_date` defined in the main script**.

  **How it works**:

  - The pipeline extracts and loads only the new or updated data based on the `created_at` or `updated_at` columns in each table. If there is no new or updated data for a given date, **the task will be skipped** to save time and resources.

  **Example**:

  - If the DAG runs on 2025-01-01, but **there are no records with `created_at` or `updated_at` on 2024-12-31 (the previous date)**, the extract and load tasks for that date **will be skipped**.

  - On the other hand, if there are records with changes on that date, the extract and load processes will run as usual.
  
    ![dag-graph]()

    Take a look at the DAG:

    - A pink flag means that on that date, **no new or updated data was found**, so the task was **skipped**.
    - A green flag means **data was detected**, so the extract or load process **ran normally**.

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
