#!/usr/bin/env python3
"""
Taiwan Freeway VD Data ETL Pipeline (Docker-friendly)
Downloads, decompresses, parses, and splits VD XML archives for a given date/range.

Usage (Docker):
    docker run --rm -v $(pwd)/data:/data my-vd-etl --date 20240530 --days 2 --zip

Author: ChatGPT refactor, 2025-06-02
"""
import argparse
import gzip
import logging
import os
import shutil
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List
import xml.etree.ElementTree as ET
import gc

import pandas as pd
import pytz
import requests
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed
from lxml import etree  # instead of xml.etree.ElementTree

# Config (env-overridable for Docker)
BASE_DIR = Path(os.getenv("BASE_DIR", "/data"))
TIMEZONE = os.getenv("TIMEZONE", "Asia/Taipei")
MAX_DL_WORKERS = int(os.getenv("MAX_DL_WORKERS", 8))
MAX_PARSE_WORKERS = int(os.getenv("MAX_PARSE_WORKERS", 16))
MIN_FILE_SIZE = int(os.getenv("MIN_FILE_SIZE", 1024))

NAMESPACE = {"ns": "http://traffic.transportdata.tw/standard/traffic/schema/"}
logging.basicConfig(level="INFO", format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")

def ensure_dir(p: Path) -> Path:
    p.mkdir(parents=True, exist_ok=True)
    return p

def get_session() -> requests.Session:
    sess = requests.Session()
    adapter = requests.adapters.HTTPAdapter(max_retries=3, pool_connections=MAX_DL_WORKERS, pool_maxsize=MAX_DL_WORKERS)
    sess.mount("http://", adapter)
    sess.mount("https://", adapter)
    return sess

def tz_yesterday(tz=TIMEZONE) -> str:
    return (datetime.now(pytz.timezone(tz)) - timedelta(days=1)).strftime("%Y%m%d")

# --- 1. Download ---
def _download_one(sess: requests.Session, url: str, dest: Path) -> bool:
    if dest.exists():
        return True
    try:
        with sess.get(url, stream=True, timeout=30) as r:
            r.raise_for_status()
            ensure_dir(dest.parent)
            with dest.open("wb") as f:
                for chunk in r.iter_content(8192):
                    f.write(chunk)
    except Exception as exc:
        logging.warning("Download failed %s – %s", dest.name, exc)
        dest.unlink(missing_ok=True)
        return False
    if dest.stat().st_size < MIN_FILE_SIZE:
        dest.unlink()
        return False
    return True

def download_day(date: str) -> Path:
    logging.info(f"Step 1: Starting download for {date}")
    base = ensure_dir(BASE_DIR / date / "compressed")
    sess = get_session()
    tasks = [
        (
            f"https://tisvcloud.freeway.gov.tw/history/motc20/VD/{date}/VDLive_{h:02d}{m:02d}.xml.gz",
            base / f"VDLive_{h:02d}{m:02d}.xml.gz",
        )
        for h in range(24) for m in range(60)
    ]
    with ThreadPoolExecutor(MAX_DL_WORKERS) as ex, tqdm(total=len(tasks), desc=f"DL {date}") as bar:
        futs = [ex.submit(_download_one, sess, url, p) for url, p in tasks]
        for fut in as_completed(futs):
            fut.result()
            bar.update(1)
    logging.info(f"Step 1: Download finished for {date}")
    return base.parent  # /data/<date>

# --- 2. Decompress ---
def decompress_day(day_path: Path) -> Path:
    logging.info(f"Step 2: Starting decompression for {day_path.name}")
    src = day_path / "compressed"
    dst = ensure_dir(day_path / "decompressed")
    gz_files = list(src.glob("*.xml.gz"))
    if not gz_files:
        logging.warning(f"No .gz files found for decompression in {src}")
    for gz in tqdm(gz_files, desc="Decompress"):
        out = dst / gz.with_suffix("").name
        if out.exists(): continue
        try:
            with gzip.open(gz, "rb") as fin, out.open("wb") as fout:
                shutil.copyfileobj(fin, fout)
        except Exception as exc:
            logging.warning("Decompress error %s – %s", gz.name, exc)
    logging.info(f"Step 2: Decompression finished for {day_path.name}")
    return dst

# --- 3. XML → CSV ---
def _parse_xml_lxml(path: Path) -> pd.DataFrame:
    NAMESPACE = "http://traffic.transportdata.tw/standard/traffic/schema/"
    NS = f"{{{NAMESPACE}}}"
    data = {}
    for _, elem in etree.iterparse(str(path), events=("end",), tag=f"{NS}VDLive"):
        vdid_elem = elem.find(f"{NS}VDID")
        vdid = vdid_elem.text if vdid_elem is not None else ""
        if not vdid:
            elem.clear()
            continue
        lanes = data.setdefault(vdid, {})
        for lane in elem.findall(f".//{NS}Lane"):
            lane_id = lane.findtext(f"{NS}LaneID", default="")
            rec = lanes.setdefault(f"L{lane_id}", {})
            rec["Speed"] = lane.findtext(f"{NS}Speed", default="")
            rec["Occupancy"] = lane.findtext(f"{NS}Occupancy", default="")
            for veh in lane.findall(f".//{NS}Vehicle"):
                vt = veh.findtext(f"{NS}VehicleType", default="")
                if not vt:
                    continue
                rec[f"{vt}_Volume"] = veh.findtext(f"{NS}Volume", default="")
                rec[f"{vt}_Vehicle_Speed"] = veh.findtext(f"{NS}Speed", default="")
        elem.clear()
    rows = []
    for vdid, lanes in data.items():
        row = {"VDID": vdid}
        for lane_id, kv in lanes.items():
            row.update({f"{lane_id}_{k}": v for k, v in kv.items()})
        rows.append(row)
    return pd.DataFrame(rows)

def xml_to_csv(day_folder: Path) -> Path:
    logging.info(f"Step 3: Starting XML to CSV for {day_folder.name}")
    src = day_folder / "decompressed"
    dst = ensure_dir(day_folder / "csv")
    xml_files = list(src.glob("*.xml"))
    if not xml_files:
        logging.warning(f"No XML files found for conversion in {src}")
    with ProcessPoolExecutor(MAX_PARSE_WORKERS) as ex, tqdm(total=len(xml_files), desc="XML→CSV") as bar:
        fut_to_name = {ex.submit(_parse_xml_lxml, p): p.name for p in xml_files}
        for fut in as_completed(fut_to_name):
            df = fut.result()
            df.to_csv(dst / fut_to_name[fut].replace(".xml", ".csv"), index=False)
            bar.update(1)
    logging.info(f"Step 3: XML to CSV finished for {day_folder.name}")
    return dst

# --- 4. Split by VDID ---
def split_by_vdid(csv_dir: Path) -> Path:
    logging.info(f"Step 4: Splitting combined CSV by VDID for {csv_dir.parent.name}")
    out_dir = ensure_dir(csv_dir.parent / "VDID")
    combined = pd.concat(
        (pd.read_csv(p).assign(file_name=p.name) for p in csv_dir.glob("*.csv")),
        ignore_index=True,
    )
    groups = list(combined.groupby("VDID"))
    with tqdm(total=len(groups), desc="Split VDID") as bar:
        for vdid, gdf in groups:
            gdf.to_csv(out_dir / f"{vdid}.csv", index=False)
            bar.update(1)
    del combined, groups
    gc.collect()
    logging.info(f"Step 4: VDID splitting finished for {csv_dir.parent.name}")
    return out_dir

# --- 5. Cleanup & Zip ---
def cleanup(day_folder: Path, keep_gz: bool, keep_xml: bool, keep_csv: bool):
    logging.info(f"Step 5: Cleaning up intermediate files for {day_folder.name}")
    if not keep_gz:
        shutil.rmtree(day_folder / "compressed", ignore_errors=True)
    if not keep_xml:
        shutil.rmtree(day_folder / "decompressed", ignore_errors=True)
    if not keep_csv:
        shutil.rmtree(day_folder / "csv", ignore_errors=True)
    logging.info(f"Step 5: Cleanup finished for {day_folder.name}")

def zip_day(day_folder: Path, delete_after: bool) -> Path:
    logging.info(f"Step 6: Zipping day folder {day_folder.name}")
    zip_path = day_folder.with_suffix(".zip")
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for f in day_folder.rglob("*"):
            zf.write(f, f.relative_to(day_folder.parent))
    if delete_after:
        shutil.rmtree(day_folder)
    logging.info(f"Step 6: Zip created at {zip_path}")
    return zip_path

# --- 6. Orchestration ---
def process_day(
    date: str,
    *,
    keep_gz: bool = False,
    keep_xml: bool = False,
    keep_csv: bool = True,
    zip_and_delete: bool = True,
) -> None:
    logging.info(f"▶ Start processing {date}")
    day_folder = download_day(date)
    decompress_day(day_folder)
    csv_dir = xml_to_csv(day_folder)
    split_by_vdid(csv_dir)
    cleanup(day_folder, keep_gz, keep_xml, keep_csv)
    if zip_and_delete:
        zip_day(day_folder, delete_after=True)
    logging.info(f"✔ Finished processing {date}")

def batch(start_date: str, days: int, **kwargs) -> None:
    dt = datetime.strptime(start_date, "%Y%m%d")
    for _ in range(days):
        logging.info(f"Batch: Start processing {dt.strftime('%Y%m%d')}")
        process_day(dt.strftime("%Y%m%d"), **kwargs)
        dt -= timedelta(days=1)
        logging.info(f"Batch: Finished processing {dt.strftime('%Y%m%d')}")

# --- CLI ---
def build_cli() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser("vd_pipeline", formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument("--date", default=tz_yesterday(), help="YYYYMMDD target date")
    p.add_argument("--days", type=int, default=1, help="Process N days backwards (inclusive)")
    p.add_argument("--keep-gz", action="store_true", help="Preserve .xml.gz after processing")
    p.add_argument("--keep-xml", action="store_true", help="Preserve raw .xml files after processing")
    p.add_argument("--keep-csv", action="store_true", help="Preserve per-file CSVs after splitting by VDID")
    p.add_argument("--no-zip", action="store_true", help="Skip zipping & deleting main folder")
    return p

if __name__ == "__main__":
    args = build_cli().parse_args()
    kwargs = dict(
        keep_gz=args.keep_gz,
        keep_xml=args.keep_xml,
        keep_csv=args.keep_csv,
        zip_and_delete=not args.no_zip,
    )
    if args.days == 1:
        process_day(args.date, **kwargs)
    else:
        batch(args.date, args.days, **kwargs)
