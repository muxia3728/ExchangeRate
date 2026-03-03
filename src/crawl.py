"""
每日汇率采集脚本
数据源：
  中间价 - 中国外汇交易中心 (chinamoney.com.cn)
  在岸CNY - Investing.com（主） / Yahoo Finance（备）
  离岸CNH - Investing.com（主） / Yahoo Finance（备）
"""

import requests
from bs4 import BeautifulSoup
import yfinance as yf
import json
import os
import re
from datetime import datetime, timedelta, timezone

# ============================================================
#  配置
# ============================================================

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}

CN_TZ = timezone(timedelta(hours=8))


def today_str():
    return datetime.now(CN_TZ).strftime("%Y-%m-%d")


def now_str():
    return datetime.now(CN_TZ).strftime("%Y-%m-%d %H:%M:%S")


def validate_rate(rate):
    return isinstance(rate, (int, float)) and 5.0 <= rate <= 9.0


# ============================================================
#  数据源1：中国外汇交易中心 - 中间价
# ============================================================

def fetch_mid_rate():
    print("[中间价] 正在从外汇交易中心获取...")
    errors = []

    # 方式1：API接口
    try:
        url = "https://www.chinamoney.com.cn/ags/ms/cm-u-bk-ccpr/CcprHis498New"
        params = {"startDate": today_str(), "endDate": today_str(), "currency": "USD/CNY"}
        resp = requests.get(url, params=params, headers=HEADERS, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            records = data.get("records", [])
            if records:
                rate = float(records[0].get("values", [0])[0])
                if validate_rate(rate):
                    print(f"[中间价] ✅ API获取成功: {rate}")
                    return rate, "外汇交易中心", None
        errors.append("API返回无有效数据")
    except Exception as e:
        errors.append(f"API异常: {str(e)[:80]}")

    # 方式2：备用API
    try:
        url2 = "https://www.chinamoney.com.cn/ags/ms/cm-u-bk-ccpr/CcsVsCn"
        resp2 = requests.get(url2, headers=HEADERS, timeout=15)
        if resp2.status_code == 200:
            data2 = resp2.json()
            records2 = data2.get("records", [])
            for record in records2:
                if record.get("foreignCurrency") == "USD":
                    rate = float(record.get("centralParity", 0))
                    if validate_rate(rate):
                        print(f"[中间价] ✅ 备用API获取成功: {rate}")
                        return rate, "外汇交易中心", None
        errors.append("备用API无有效数据")
    except Exception as e:
        errors.append(f"备用API异常: {str(e)[:80]}")

    print(f"[中间价] ❌ 全部失败: {errors}")
    return None, None, f"中间价获取失败: {'; '.join(errors)}"


# ============================================================
#  数据源2：Investing.com - 在岸CNY / 离岸CNH
# ============================================================

def fetch_investing(pair_type):
    label = "在岸CNY" if pair_type == "cny" else "离岸CNH"
    slug = "usd-cny" if pair_type == "cny" else "usd-cnh"
    print(f"[{label}] 正在从 Investing.com 获取...")

    try:
        url = f"https://cn.investing.com/currencies/{slug}-historical-data"
        custom_headers = {
            **HEADERS,
            "Referer": "https://cn.investing.com/",
        }
        resp = requests.get(url, headers=custom_headers, timeout=15)

        if resp.status_code != 200:
            return None, None, f"Investing HTTP {resp.status_code}"

        soup = BeautifulSoup(resp.text, "lxml")

        # 方式1：查找历史数据表格
        tables = soup.find_all("table")
        for table in tables:
            header_text = table.get_text()
            if "收盘" in header_text or "Close" in header_text:
                rows = table.find_all("tr")
                for row in rows[1:3]:
                    cells = row.find_all("td")
                    if len(cells) >= 2:
                        close_text = cells[1].get_text(strip=True).replace(",", "")
                        try:
                            rate = float(close_text)
                            if validate_rate(rate):
                                print(f"[{label}] ✅ Investing获取成功: {rate}")
                                return rate, "Investing.com", None
                        except ValueError:
                            continue

        # 方式2：从页面脚本中提取
        scripts = soup.find_all("script")
        for script in scripts:
            text = script.string or ""
            patterns = [
                r'"last(?:_close|Price|_price)"\s*:\s*"?([\d.]+)"?',
                r'"close"\s*:\s*"?([\d.]+)"?',
            ]
            for pattern in patterns:
                matches = re.findall(pattern, text, re.IGNORECASE)
                if matches:
                    rate = float(matches[0])
                    if validate_rate(rate):
                        print(f"[{label}] ✅ Investing(Script)获取成功: {rate}")
                        return rate, "Investing.com", None

        return None, None, f"Investing页面解析未找到{label}数据"

    except Exception as e:
        return None, None, f"Investing异常: {str(e)[:80]}"


# ============================================================
#  数据源3（备用）：Yahoo Finance
# ============================================================

def fetch_yahoo(pair_type):
    label = "在岸CNY" if pair_type == "cny" else "离岸CNH"
    ticker = "CNY=X" if pair_type == "cny" else "CNH=X"
    print(f"[{label}] 正在从 Yahoo Finance 备用源获取...")

    try:
        tk = yf.Ticker(ticker)
        hist = tk.history(period="5d")

        if hist.empty:
            return None, None, f"Yahoo Finance返回空数据"

        rate = round(float(hist.iloc[-1]["Close"]), 4)
        if validate_rate(rate):
            print(f"[{label}] ✅ Yahoo获取成功: {rate}")
            return rate, "Yahoo Finance", None
        else:
            return None, None, f"Yahoo数据校验失败: {rate}"

    except Exception as e:
        return None, None, f"Yahoo异常: {str(e)[:80]}"


# ============================================================
#  主流程
# ============================================================

def collect_all():
    print("=" * 60)
    print(f"开始采集汇率数据 {now_str()}")
    print("=" * 60)

    result = {
        "date": today_str(),
        "mid_rate": None,
        "onshore_cny": None,
        "offshore_cnh": None,
        "source_mid": "",
        "source_cny": "",
        "source_cnh": "",
        "errors": "",
        "updated_at": now_str(),
    }

    all_errors = []

    # 1. 中间价
    rate, source, error = fetch_mid_rate()
    result["mid_rate"] = rate
    result["source_mid"] = source or ""
    if error:
        all_errors.append(error)

    # 2. 在岸 CNY（主源 Investing → 备用 Yahoo）
    rate, source, error = fetch_investing("cny")
    if rate is not None:
        result["onshore_cny"] = rate
        result["source_cny"] = source
    else:
        investing_error = error
        print(f"[在岸CNY] 主源失败，切换Yahoo Finance...")
        rate, source, error = fetch_yahoo("cny")
        result["onshore_cny"] = rate
        result["source_cny"] = source or ""
        if error:
            all_errors.append(f"在岸CNY: {investing_error} -> {error}")

    # 3. 离岸 CNH（主源 Investing → 备用 Yahoo）
    rate, source, error = fetch_investing("cnh")
    if rate is not None:
        result["offshore_cnh"] = rate
        result["source_cnh"] = source
    else:
        investing_error = error
        print(f"[离岸CNH] 主源失败，切换Yahoo Finance...")
        rate, source, error = fetch_yahoo("cnh")
        result["offshore_cnh"] = rate
        result["source_cnh"] = source or ""
        if error:
            all_errors.append(f"离岸CNH: {investing_error} -> {error}")

    # 汇总错误
    result["errors"] = " | ".join(all_errors) if all_errors else ""

    print("=" * 60)
    print(f"采集结果:")
    print(f"  日期:    {result['date']}")
    print(f"  中间价:  {result['mid_rate']}  来源: {result['source_mid'] or '失败'}")
    print(f"  在岸CNY: {result['onshore_cny']}  来源: {result['source_cny'] or '失败'}")
    print(f"  离岸CNH: {result['offshore_cnh']}  来源: {result['source_cnh'] or '失败'}")
    if result["errors"]:
        print(f"  错误: {result['errors']}")
    print("=" * 60)

    return result


def save_result(result):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.dirname(script_dir)
    data_dir = os.path.join(repo_root, "data")
    os.makedirs(data_dir, exist_ok=True)

    # 写入 latest.json
    filepath = os.path.join(data_dir, "latest.json")
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"结果已保存到 {filepath}")

    # 追加到 history.csv
    csv_path = os.path.join(data_dir, "history.csv")
    file_exists = os.path.exists(csv_path)
    with open(csv_path, "a", encoding="utf-8") as f:
        if not file_exists:
            f.write("日期,中间价,在岸CNY,离岸CNH,来源_中间价,来源_CNY,来源_CNH,错误,更新时间\n")
        f.write(
            f"{result['date']},"
            f"{result['mid_rate'] or ''},"
            f"{result['onshore_cny'] or ''},"
            f"{result['offshore_cnh'] or ''},"
            f"{result['source_mid']},"
            f"{result['source_cny']},"
            f"{result['source_cnh']},"
            f"\"{result['errors']}\","
            f"{result['updated_at']}\n"
        )
    print(f"历史记录已追加到 {csv_path}")


if __name__ == "__main__":
    result = collect_all()
    save_result(result)
