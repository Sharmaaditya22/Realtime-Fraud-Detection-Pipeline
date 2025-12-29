# 🛡️ Real-Time Fraud Detection System

![Python](https://img.shields.io/badge/Python-3.9%2B-blue)
![Spark](https://img.shields.io/badge/Apache%20Spark-Streaming-orange)
![Kafka](https://img.shields.io/badge/Kafka-Real--Time-black)
![Redis](https://img.shields.io/badge/Redis-Cache-red)
![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-ff4b4b)

An end-to-end data engineering pipeline that detects fraudulent financial transactions in real-time. It processes high-volume transaction streams using **Apache Kafka** and **Spark Structured Streaming**, identifies suspicious patterns, and visualizes alerts instantly on a **Streamlit** dashboard.

---

## 📖 Overview

This project simulates a real-world fraud detection engine. It is designed to handle "late-arriving data" using watermarking and separates storage into a **Speed Layer** (Redis) for instant alerts and a **Batch/History Layer** (Delta Lake) for long-term storage.

**Key Features:**
* **Real-Time Ingestion:** Generates synthetic transaction data with realistic fraud patterns (e.g., "Evil Corp", high amounts).
* **Stream Processing:** PySpark job with stateful processing and 5-minute watermarking to handle late data.
* **Hybrid Storage Architecture:**
    * **Redis:** Low-latency storage for the live dashboard.
    * **Delta Lake:** ACID-compliant storage for historical analysis.
* **Interactive "Mission Control":** A Streamlit app to start/stop simulations and monitor fraud as it happens.

---

## 🏗️ High-Level Architecture

Below is the High-Level Design (HLD) of the system data flow.

![High Level Design](https://github.com/Sharmaaditya22/Realtime-Fraud-Detection-Pipeline/blob/91ec42dc2cc423b66e92d96beb998fc6a1626b37/Images/architecture_diagram.png)

**The Pipeline Flow:**
1.  **Producer (`python_producer.py`):** Generates fake credit card transactions and pushes them to Kafka.
2.  **Message Broker (Kafka):** Queues the raw data reliably.
3.  **Processing Engine (`realtime_fraud_detection.py`):**
    * Reads from Kafka.
    * Parses JSON and applies a schema.
    * **Enriches data:** Joins with a broadcasted "Bad Merchant" blacklist.
    * **Watermarking:** Handles data up to 5 minutes late.
    * **Fraud Logic:** Filters for transactions > $10,000 or from Blacklisted Merchants.
4.  **Sinks:**
    * **Redis (Speed Layer):** Valid fraud alerts are pushed here for the dashboard.
    * **Delta Lake (Batch Layer):** All transactions are archived here for audit/history.
5.  **Frontend (`dashboard.py`):** Polls Redis to display live metrics and alerts.

---

## 🛠️ Prerequisites

Before running this project, ensure you have the following installed:

* **Python 3.9+**
* **Docker Desktop** (for running Kafka and Redis)
* **Java 8 or 11** (Required for PySpark)

---

## 🚀 Setup Guide

### 1. Clone the Repository

```bash
git clone [https://github.com/Sharmaaditya22/Realtime-Fraud-Detection-Pipeline.git](https://github.com/Sharmaaditya22/Realtime-Fraud-Detection-Pipeline.git)
cd Realtime-Fraud-Detection-Pipeline
```

### 2. Install Dependencies
We recommend creating a virtual environment first.

Bash

Using pip
```bash
pip install -r requirements.txt
```

OR using uv (faster)
```bash
uv pip install -r requirements.txt
```

### 3. Start Infrastructure (Kafka & Redis)
Navigate to the docker folder and start the services.

Bash
```bash
cd kafka_docker_file
docker-compose up -d
cd ..
```

(Ensure Zookeeper is running on port 2181, Kafka on 9092, and Redis on 6379)

## ▶️ How to Run
You will need two or three separate terminals to run the full pipeline.

### Terminal 1: The Spark Processor
This initiates the listening stream. It will wait for incoming data.

Bash
```bash
python realtime_fraud_detection.py
```
Wait until you see the log message: Waiting for data...

### Terminal 2: The Dashboard
This launches the web interface ("Mission Control").

Bash
```bash
streamlit run dashboard.py
```
The app will open in your browser at http://localhost:8501.

Click the "🚀 Start Simulation" button in the sidebar to automatically start generating data.

### Terminal 3 (Optional): Manual Producer
If you prefer to run the data generator manually instead of using the dashboard button:

python python_producer.py
## 📂 Project Structure
Plaintext

├── dashboard.py                # Streamlit Frontend ("Mission Control")
├── python_producer.py          # Data Generator (Simulates Transactions)
├── realtime_fraud_detection.py # Main Spark Structured Streaming Job
├── kafka_docker_file/          # Docker Compose setup for Kafka/Redis
├── requirements.in             # Top-level dependencies (for humans)
├── requirements.txt            # Frozen dependencies (for machines)
├── jars/                       # Folder for Delta Lake JARs
├── checkpoints/                # Spark Checkpoint folder (auto-generated)
└── lakehouse/                  # Delta Lake storage folder (auto-generated)
📸 Screenshots

## Live Fraud Dashboard
![Ouput1](https://github.com/Sharmaaditya22/Realtime-Fraud-Detection-Pipeline/blob/91ec42dc2cc423b66e92d96beb998fc6a1626b37/Images/Output1.png)
![Ouput2](https://github.com/Sharmaaditya22/Realtime-Fraud-Detection-Pipeline/blob/91ec42dc2cc423b66e92d96beb998fc6a1626b37/Images/Output2.png)
