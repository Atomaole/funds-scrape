# Thai Mutual Fund Data Aggregation Pipeline

![Python](https://img.shields.io/badge/Python-3.9%2B-blue?style=flat-square)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-14%2B-elephant?style=flat-square)
![Docker](https://img.shields.io/badge/Docker-Container-blue?style=flat-square)
![Status](https://img.shields.io/badge/Status-Production%20Ready-success?style=flat-square)

## 📖 Executive Summary

This repository houses a centralized **Data Aggregation and Normalization System** tailored for the Thai Mutual Fund market.

### 🚨 IMPORTANT — SINGLE ENTRY SYSTEM

> **The entire system is driven by a SINGLE PRIMARY ENTRY POINT — `master_runner.py`**

`master_runner.py` is the brain, heart, and control center of this pipeline. **No other script should be executed directly.**

The architecture follows a **Single-Entry Orchestrator Model**, where `master_runner.py` manages the entire lifecycle: scraping data from multiple sources (Finnomena, WealthMagik, SEC), transforming and merging datasets, and upserting results into a PostgreSQL warehouse.

The system is engineered for resilience and autonomy, featuring **resume-on-failure logic**, **date-aware scheduling** (Daily vs Monthly), and **configurable concurrency modes**.

---

## 🏗️ System Architecture

The pipeline operates under a centralized orchestration model:

| Component | Responsibility |
| :--- | :--- |
| **🔥 Orchestrator (CORE)** | `master_runner.py` — **THE ONLY ENTRY POINT.** Controls execution order, scheduling logic, concurrency mode, recovery, and lifecycle management. |
| **Ingestion Layer** | Headless Selenium / Requests workers extracting NAV, Bid/Offer, Holdings, and Risk Metrics. |
| **Transformation** | `merge_funds.py` — Vectorized Pandas operations to normalize and merge heterogeneous datasets. |
| **Persistence** | `db_loader.py` — SQLAlchemy-based loader ensuring atomic transactions and idempotent upserts via `COALESCE`. |
| **Infrastructure** | Dockerized PostgreSQL database for scalable and reliable storage. |

---

## 🚀 Deployment Guide

### 1. Environment Prerequisites
Ensure the following are installed:
* Python 3.9+
* Docker & Docker Compose
* Mozilla Firefox (Latest)

### 2. Infrastructure Setup (Database)
```bash
docker-compose up -d

```

*Note: The database schema is automatically initialized using `init.sql` on the first connection.*

### 3. Application Dependencies

```bash
pip install -r requirements.txt

```

### 4. Configuration

Verify database credentials in `db_loader.py`:

```python
DB_USER = "admin"
DB_PASS = "password"
DB_HOST = "localhost"
DB_PORT = "5432"

```

---

## ⚡ Execution (Single Entry Execution Model)

⚠️ **DO NOT run individual scripts manually.**
This system strictly enforces a Single Entry Execution Model. Only `master_runner.py` is allowed to be executed directly.

```bash
python master_runner.py

```

*All other modules (scrapers, `merge_funds.py`, `db_loader.py`) are internal components invoked exclusively by `master_runner.py`.*

### Runtime Configuration

Adjust execution behavior via the `MODE` constant in `master_runner.py`:

* **`MODE = 1` (Sequential):** Maximum stability, lowest resource usage.
* **`MODE = 2` (Hybrid – Recommended):** Critical tasks synchronous, heavy tasks in background.
* **`MODE = 3` (Parallel):** Full concurrency for high-bandwidth environments.

---

## 🗄️ Data Model

The system populates a normalized relational schema (`funds_db`):

| Table | Description |
| --- | --- |
| `funds_master_info` | Static fund metadata, AMC, policies, inception dates |
| `funds_daily` | Time-series NAV, AUM, Bid, and Offer prices |
| `funds_statistics` | Risk metrics (Sharpe, Alpha, Beta, Max Drawdown) |
| `funds_holding` | Portfolio composition and holdings |
| `funds_allocations` | Asset class and geographic allocations |
| `funds_fee` | Comprehensive fee structures |

---

## 🛡️ Reliability Features

* **Smart Resume:** Execution resumes automatically from last successful checkpoint.
* **Date-Aware Scheduling:** Differentiates full monthly scrape vs incremental daily updates.
* **Data Integrity:** Uses `ON CONFLICT DO UPDATE` with `COALESCE` to prevent data loss.

---

#### ระบบรวมและประมวลผลข้อมูลกองทุนรวมไทย (Thai Language Section)

### 📖 ภาพรวมระบบ

Repository นี้เป็นระบบ **รวมข้อมูลและปรับมาตรฐานข้อมูล** สำหรับตลาดกองทุนรวมไทย โดยใช้สถาปัตยกรรมแบบ **Single-Entry Orchestrator**

### 🚨 สำคัญมาก — ระบบทางเข้าเดียว

ระบบทั้งหมดถูกควบคุมโดยไฟล์หลักเพียงไฟล์เดียวคือ `master_runner.py`
❌ **อย่ารันไฟล์อื่นโดยตรง**

`master_runner.py` คือศูนย์ควบคุมของระบบ ทำหน้าที่ตั้งแต่ดึงข้อมูลจากหลายแหล่ง รวมข้อมูล ประมวลผล และบันทึกลงฐานข้อมูล PostgreSQL
ระบบถูกออกแบบให้ **เสถียร, ทำงานอัตโนมัติ, และ สามารถ resume ได้เมื่อเกิดข้อผิดพลาด**

---

### 🏗️ โครงสร้างระบบ

| ส่วนประกอบ | หน้าที่ |
| --- | --- |
| **🔥 Orchestrator (CORE)** | `master_runner.py` — จุดเริ่มต้นเพียงจุดเดียวของระบบ ควบคุม flow ทั้งหมด |
| **Ingestion Layer** | ดึงข้อมูลด้วย Selenium / Requests |
| **Transformation** | `merge_funds.py` รวมและปรับโครงสร้างข้อมูล |
| **Persistence** | `db_loader.py` บันทึกข้อมูลลง PostgreSQL อย่างปลอดภัย |
| **Infrastructure** | PostgreSQL ผ่าน Docker |

---

### ⚡ การรันระบบ

⚠️ ระบบนี้อนุญาตให้รันเพียงไฟล์เดียวเท่านั้น

```bash
python master_runner.py

```

*ไฟล์อื่นทั้งหมดเป็นโมดูลภายในที่ถูกเรียกโดย `master_runner.py` เท่านั้น*

---

*Disclaimer: This project is intended for data aggregation purposes only.*