# Taiwan Freeway VD Data ETL Pipeline

**Author:** Kuan-Yuan Chen
**Assisted by:** ChatGPT
**Last updated:** 2025-06-02

---

## Overview

This project is a robust ETL (Extract, Transform, Load) pipeline for historical Taiwan Freeway Vehicle Detector (VD) data, suitable for both research and production use. It is designed for automation, parallelization, and efficient processing of large volumes of traffic data.
**Out-of-the-box Docker compatibility** ensures seamless deployment on Windows, Linux, or NAS (Synology) environments.

* **Downloads**: Fetches per-minute gzipped XML data from the Taiwan Freeway Open Data platform for a specified date range.
* **Decompresses**: Parallelized decompression of all `.xml.gz` files.
* **Parses**: Converts raw XMLs to tabular CSVs using fast, memory-efficient lxml parsing.
* **Splits**: Organizes output by VDID (Vehicle Detector ID) for downstream analytics.
* **Cleans up**: Optionally removes intermediate files.
* **Zips**: Packages each processed day for easy archiving or transfer.

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
* [FAQ / Tips](#faq--tips)
* [References](#references)

---

## Features

* 🚦 **End-to-end ETL for Taiwan freeway VD data**
* ⚡ **Multi-threaded and multi-process**: Download and parsing are highly parallelized for speed
* 🧹 **Automated cleanup**: Optionally remove intermediate files
* 📦 **Daily output zipping**: Easy archiving/sharing
* 🐳 **Docker-friendly**: Mount your local data directory for easy persistence
* 🛠️ **Configurable**: Choose how many days to process, retention of files, workers, timezone, etc.

---

## Folder Structure

**After processing, your data directory will look like:**

```
data/
├─ 20240530.zip                # Zipped output for the day (optional)
├─ 20240530/                   # Unzipped folder (may be auto-removed if zipped)
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
* **Docker** (recommended for production and cross-platform use)

**Python packages** (automatically installed in Docker):

* `pandas`
* `requests`
* `lxml`
* `tqdm`
* `pytz`

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

   * This will process data for **2024-05-30** and **2024-05-29** (2 days backwards), zipping results and storing outputs under `./data`.

4. **(Optional) Customizing Environment Variables:**

   You can override environment variables using `-e` flags:

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

   * (By default, outputs are written to `./output` if not using Docker.)

---

## Command-line Usage

```bash
python main.py [--date YYYYMMDD] [--days N] [--keep-gz] [--keep-xml] [--keep-csv] [--no-zip]
```

* `--date`: Target date (default: yesterday in Asia/Taipei timezone)
* `--days`: Number of days to process backwards (default: 1)
* `--keep-gz`: Preserve original `.xml.gz` files after processing
* `--keep-xml`: Preserve decompressed `.xml` files after processing
* `--keep-csv`: Preserve per-minute CSV files (default: True)
* `--no-zip`: Skip zipping the main output folder for each day

**Examples:**

* Process yesterday’s data and keep only final VDID split:

  ```bash
  python main.py
  ```

* Process 3 days, keep all intermediates, don’t zip:

  ```bash
  python main.py --days 3 --keep-gz --keep-xml --keep-csv --no-zip
  ```

---

## Environment Variables

All can be overridden in Docker (`-e`), `.env`, or shell:

| Variable            | Default                                  | Description                                 |
| ------------------- | ---------------------------------------- | ------------------------------------------- |
| `BASE_DIR`          | `/data` (Docker)<br>`./output` (Windows) | Output base directory                       |
| `TIMEZONE`          | `Asia/Taipei`                            | Local timezone for date calculation         |
| `MAX_DL_WORKERS`    | `8`                                      | Parallel download workers                   |
| `MAX_PARSE_WORKERS` | `16`                                     | Parallel XML parsing workers                |
| `MIN_FILE_SIZE`     | `1024`                                   | Ignore gz files smaller than this (corrupt) |

---

## Pipeline Stages

**For each day:**

1. **Download:**
   Download 1,440 gzipped XMLs (`VDLive_HHMM.xml.gz` for every minute in 24hr) from the official source.

2. **Decompress:**
   Parallel decompress `.xml.gz` to `.xml`.

3. **Parse (XML → CSV):**

   * Use **lxml** for efficient, memory-safe streaming.
   * Output per-minute CSVs.

4. **Split by VDID:**

   * Merge all minute CSVs, group by `VDID` (Vehicle Detector ID).
   * Output **one CSV per VDID** for the day.

5. **Cleanup:**
   Remove intermediates unless you set the `--keep-*` flags.

6. **Zip (optional):**
   Zip the whole processed day (recommended for archiving).

---

## Customization

* Change the **output path** by setting `BASE_DIR`.
* Increase/decrease parallelism by tuning `MAX_DL_WORKERS` and `MAX_PARSE_WORKERS`.
* For advanced users, the code can be modularized further or integrated into a larger Airflow or Prefect pipeline.

---

## FAQ & Tips

### Q: Where can I find the processed data?

> Check your mounted data directory (e.g., `./data/20240530/VDID/`).
> Each file is `<VDID>.csv` — these are ready for analysis.

### Q: What if a download is incomplete or corrupt?

> Files smaller than `MIN_FILE_SIZE` are automatically discarded and re-downloaded.
> Robust connection retries are built in.

### Q: Can I process more than 1 day at a time?

> Yes, use `--days N`. The tool will walk backwards day by day.

### Q: How can I speed up processing?

> Increase the worker environment variables (`MAX_DL_WORKERS`, `MAX_PARSE_WORKERS`).
> (Limited by CPU, RAM, and your Internet bandwidth.)

### Q: How big is the output?

> A single day can be several hundred MBs zipped.
> Final per-VDID CSVs are much smaller and suitable for direct analysis.

### Q: How do I debug or add logging?

> Logging is output to stdout and visible via `docker logs`.

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

* Original author: **Kuan-Yuan Chen**
* Assisted by: **ChatGPT**
---