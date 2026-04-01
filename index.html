# server.py（最终稳定版：解决下载不弹出问题）

from flask import Flask, request, send_file, jsonify, send_from_directory
import io
import zipfile
import requests
import pandas as pd
import urllib3
import time
import json
from datetime import datetime, timedelta, timezone

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

app = Flask(__name__)

SYMBOL = "ETHUSDT"
VISION_BASE_URL = "https://data.binance.vision/data/futures/um/daily/aggTrades"


# ================= 日期展开 =================
def expand_date_range(start, end):
    s = datetime.strptime(start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    e = datetime.strptime(end, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    days = []
    cur = s
    while cur <= e:
        days.append(cur)
        cur += timedelta(days=1)
    return days


# ================= 单日下载 =================
def download_one_day(date_obj):
    date_str = date_obj.strftime("%Y-%m-%d")
    zip_name = f"{SYMBOL}-aggTrades-{date_str}.zip"
    url = f"{VISION_BASE_URL}/{SYMBOL}/{zip_name}"

    try:
        r = requests.get(url, verify=False, timeout=60)
        if r.status_code != 200:
            return None, f"❌ 下载失败 {date_str}"

        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            name = z.namelist()[0]
            with z.open(name) as f:
                df = pd.read_csv(
                    f,
                    header=None,
                    names=[
                        'agg_id','price','quantity',
                        'first_trade_id','last_trade_id',
                        'timestamp','is_buyer_maker','ignore'
                    ],
                    low_memory=False
                )

        if not str(df.iloc[0, 0]).isdigit():
            df = df.iloc[1:]

        df = df.astype({
            'agg_id': int,
            'price': float,
            'quantity': float,
            'first_trade_id': int,
            'last_trade_id': int,
            'timestamp': int
        })

        csv_bytes = df.to_csv(index=False).encode("utf-8")
        return csv_bytes, f"{SYMBOL}_{date_str}.csv"

    except Exception as e:
        return None, f"❌ 异常 {date_str}: {str(e)}"


# ================= 首页 =================
@app.route("/")
def home():
    return send_from_directory(".", "index.html")


# ================= 下载接口 =================
@app.route("/download", methods=["POST"])
def download():
    try:
        data = json.loads(request.form.get("data"))
        ranges = data.get("ranges", [])

        if not ranges:
            return "no ranges",
