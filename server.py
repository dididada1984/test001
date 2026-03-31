# server.py（Render部署版：多日期段 → 打包ZIP → 浏览器下载）

from flask import Flask, request, send_file, jsonify
import io
import zipfile
import requests
import pandas as pd
import urllib3
import time
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

        # 处理异常header
        if not str(df.iloc[0, 0]).isdigit():
            df = df.iloc[1:]

        # 转类型
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
    return "✅ Data Downloader Running"


# ================= 下载接口 =================
@app.route("/download", methods=["POST"])
def download():
    try:
        data = request.json
        ranges = data.get("ranges", [])

        if not ranges:
            return jsonify({"error": "no date ranges"}), 400

        zip_buffer = io.BytesIO()
        zf = zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED)

        logs = []

        for start, end in ranges:
            for d in expand_date_range(start, end):
                file_bytes, msg = download_one_day(d)

                logs.append(msg)

                if file_bytes:
                    filename = msg if msg.endswith(".csv") else msg.replace("❌ ", "") + ".csv"
                    zf.writestr(filename, file_bytes)

                time.sleep(0.2)

        zf.close()
        zip_buffer.seek(0)

        return send_file(
            zip_buffer,
            as_attachment=True,
            download_name="data.zip",
            mimetype="application/zip"
        )

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ================= 启动 =================
if __name__ == "__main__":
    app.run()
