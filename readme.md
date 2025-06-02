# Taiwan Freeway VD Data ETL Pipeline

**Author:** Kuan-Yuan Chen, Tzu-Chi Huang

**Assisted by:** ChatGPT o3

**Last updated:** 2025-06-02

![Docker Ready](https://img.shields.io/badge/docker-ready-blue)
![License: MIT](https://img.shields.io/badge/license-MIT-green)

---

## Overview

This project is a robust ETL (Extract, Transform, Load) pipeline for historical Taiwan Freeway Vehicle Detector (VD) data. It is designed for automation, parallelization, and efficient processing of large traffic datasets.
**Out-of-the-box Docker compatibility** ensures seamless deployment on Windows, Linux, or NAS (Synology).

* **Downloads:** Per-minute gzipped XML data from the Taiwan Freeway Open Data platform for a specified date range
* **Decompresses:** Parallelized decompression of all `.xml.gz` files
* **Parses:** Converts raw XMLs to tabular CSVs using fast, memory-efficient lxml parsing
* **Splits:** Organizes output by VDID (Vehicle Detector ID)
* **Cleans up:** Optionally removes intermediate files
* **Zips:** Packages each processed day for easy archiving or transfer

---

## Table of Contents

* [Features](#features)
* [Folder Structure](#folder-structure)
* [Prerequisites](#prerequisites)
* [Quick Start (Docker)](#quick-start-docker)
* [Quick Start (Python)](#quick-start-python)
* [Command-line Usage](#command-line-usage)
* [Environment Variables](#environment-variables)
* [Pipeline Stages](#pipeline-stages)
* [Customization](#customization)
* [Run on Docker](#run-on-docker)
* [FAQ & Tips](#faq--tips)
* [References](#references)
* [License](#license)
* [Credits](#credits)

---

## Features

* 🚦 **End-to-end ETL** for Taiwan freeway VD data
* ⚡ **Multi-threaded and multi-process:** Fast download & parsing
* 🧹 **Automated cleanup:** Optionally remove intermediate files
* 📦 **Daily output zipping:** Easy archiving/sharing
* 🐳 **Docker-friendly:** Simple volume mount for persistence
* 🛠️ **Configurable:** Days, file retention, workers, timezone, etc.

---

## Folder Structure

**After processing, your data directory will look like:**

```
data/
├─ 20240530.zip                # Zipped output for the day (optional)
├─ 20240530/                   # Unzipped folder (auto-removed if zipped)
│   ├─ compressed/             # Raw .xml.gz files (optional)
│   ├─ decompressed/           # Raw .xml files (optional)
│   ├─ csv/                    # Per-minute CSVs (optional)
│   └─ VDID/                   # <— Final output: 1 CSV per VDID
├─ 20240529.zip
├─ ...
```

---

## Prerequisites

* **Python 3.8+** (if not using Docker)
* **Docker** (recommended)

**Python packages** (auto-installed in Docker):

```
pandas
requests
lxml
tqdm
pytz
```

---

## Quick Start (Docker)

1. **Build the Docker image:**

   ```bash
   docker build -t my-vd-etl .
   ```

2. **Create a data directory for outputs:**

   ```bash
   mkdir -p $(pwd)/data
   ```

3. **Run the ETL for a given date (e.g., 2024-05-30):**

   ```bash
   docker run --rm -v $(pwd)/data:/data my-vd-etl --date 20240530 --days 2 --zip
   ```

   * This processes **2024-05-30** and **2024-05-29**, storing outputs under `./data`.

4. **Customizing environment variables:**

   ```bash
   docker run --rm -v $(pwd)/data:/data \
       -e MAX_DL_WORKERS=12 -e TIMEZONE=Asia/Taipei my-vd-etl --date 20240530
   ```

---

## Quick Start (Python)

1. **Install dependencies:**

   ```bash
   pip install -r requirements.txt
   ```

2. **Run directly:**

   ```bash
   python main.py --date 20240530 --days 2 --zip
   ```

   (Outputs go to `./output` by default.)

---

## Command-line Usage

```bash
python main.py [--date YYYYMMDD] [--days N] [--keep-gz] [--keep-xml] [--keep-csv] [--no-zip]
```

* `--date`         Target date (default: yesterday in Asia/Taipei)
* `--days`         Number of days to process backwards (default: 1)
* `--keep-gz`      Preserve original `.xml.gz` files
* `--keep-xml`     Preserve decompressed `.xml` files
* `--keep-csv`     Preserve per-minute CSV files (default: True)
* `--no-zip`       Skip zipping the output folder

**Examples:**

```bash
python main.py
python main.py --days 3 --keep-gz --keep-xml --keep-csv --no-zip
```

---

## Environment Variables

Override in Docker (`-e`), `.env`, or your shell:

| Variable            | Default                                | Description                                 |
| ------------------- | -------------------------------------- | ------------------------------------------- |
| `BASE_DIR`          | `/data` (Docker), `./output` (Windows) | Output base directory                       |
| `TIMEZONE`          | `Asia/Taipei`                          | Local timezone for date calculation         |
| `MAX_DL_WORKERS`    | `8`                                    | Parallel download workers                   |
| `MAX_PARSE_WORKERS` | `16`                                   | Parallel XML parsing workers                |
| `MIN_FILE_SIZE`     | `1024`                                 | Ignore gz files smaller than this (corrupt) |

---

## Pipeline Stages

1. **Download:**
   Download 1,440 gzipped XMLs (`VDLive_HHMM.xml.gz`, every minute of the day).

2. **Decompress:**
   Parallel decompress `.xml.gz` to `.xml`.

3. **Parse (XML → CSV):**

   * Uses lxml for efficient streaming.
   * Outputs per-minute CSVs.

4. **Split by VDID:**

   * Merges minute CSVs, groups by `VDID`.
   * Outputs one CSV per VDID.

5. **Cleanup:**
   Removes intermediates unless you use the `--keep-*` flags.

6. **Zip (optional):**
   Zips the processed day's folder.

---

## Customization

* Change the **output path** using `BASE_DIR`
* Adjust parallelism with `MAX_DL_WORKERS` and `MAX_PARSE_WORKERS`
* For advanced automation, the pipeline can be called from orchestration tools

---

## Run on Docker

### Example Dockerfile

```dockerfile
FROM python:3.11-slim
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc libxml2-dev libxslt1-dev && \
    rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
RUN useradd -ms /bin/bash syno && chown -R syno:syno /app
USER syno
ENTRYPOINT ["python", "main.py"]
```

### requirements.txt (minimal)

```
pandas
requests
lxml
tqdm
pytz
```

---

### Sample Output File

#### Example: `VDID/0001.csv`

| VDID | L1\_Speed | L1\_Occupancy | L1\_S\_Volume | L1\_S\_Vehicle\_Speed | ... |
| ---- | --------- | ------------- | ------------- | --------------------- | --- |
| 0001 | 78        | 5             | 12            | 78                    | ... |
| 0001 | 76        | 7             | 14            | 76                    | ... |

* Each row = snapshot at a minute for that VDID
* **Columns:**

  * `VDID`: Vehicle detector ID
  * `L1_Speed`: Lane 1, average speed
  * `L1_Occupancy`: Lane 1, occupancy %
  * `L1_S_Volume`: Lane 1, small car count
  * `L1_S_Vehicle_Speed`: Lane 1, small car average speed
  * ...etc for all lanes/vehicle types

---

### Example Docker Run

```bash
docker build -t my-vd-etl .
docker run --rm -v $(pwd)/data:/data my-vd-etl --date 20240530 --days 2 --zip
```

---

## FAQ & Tips

> **Where is the processed data?**
> In your mounted data directory (e.g., `./data/20240530/VDID/`).

> **What if a download is corrupt?**
> Files smaller than `MIN_FILE_SIZE` are discarded/retried.

> **Process more than 1 day?**
> Use `--days N`, tool walks backwards day by day.

> **How to speed up?**
> Increase `MAX_DL_WORKERS`, `MAX_PARSE_WORKERS` (limited by CPU/network).

> **How big is output?**
> Several hundred MBs per day zipped; final VDID CSVs are much smaller.

> **Debug logging?**
> Output to stdout, visible via `docker logs`.

---

## References

* [Taiwan Freeway Bureau Open Data Portal](https://tisvcloud.freeway.gov.tw/history/motc20/VD/)
* [pandas documentation](https://pandas.pydata.org/)
* [lxml documentation](https://lxml.de/)

---

## License

MIT

---

## Credits

* Original author: **Kuan-Yuan Chen**, **Tzu-Chi Huang**
* Assisted by: **ChatGPT**

