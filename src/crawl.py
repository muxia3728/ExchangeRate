"""
每日汇率采集脚本
采集完成后：
  1. 保存到 data/latest.json（备份）
  2. POST 到飞书工作流 Webhook（自动写入多维表格）
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

HEADERS_COMMON = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}

CN_TZ = timezone(timedelta(hours=8))

# 飞书 Webhook 地址（从环境变量读取）
FEISHU_WEBHOOK_URL = os.environ.get("FEISHU_WEBHOOK_URL", "")


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
    date = today_str()

    try:
        url = "https://www.chinamoney.com.cn/ags/ms/cm-u-bk-ccpr/CcprHisNew"
        params = {
            "startDate": date,
            "endDate": date,
            "currency": "USD/CNY",
        }
        resp = requests.get(url, params=params, headers=HEADERS_COMMON, timeout=15)

        if resp.status_code == 200:
            data = resp.json()
            records = data.get("records", [])
            if records and records[0].get("values"):
                rate_str = records[0]["values"][0]
                rate = float(rate_str)
                if validate_rate(rate):
                    print(f"[中间价] ✅ 获取成功: {rate}")
                    return rate, "外汇交易中心", None
                else:
                    errors.append(f"数据校验失败: {rate}")
            else:
                errors.append(f"当日({date})无数据,可能非工作日或未公布")
        else:
            errors.append(f"HTTP {resp.status_code}")

    except Exception as e:
        errors.append(f"请求异常: {str(e)[:80]}")

    print(f"[中间价] ❌ 失败: {errors}")
    return None, None, f"中间价获取失败: {'; '.join(errors)}"


# ============================================================
#  数据源2：Investing.com
# ============================================================

def fetch_investing(pair_type):
    """从 Investing.com 页面源码中的 historicalDataStore 提取收盘价"""
    label = "在岸CNY" if pair_type == "cny" else "离岸CNH"
    slug = "usd-cny" if pair_type == "cny" else "usd-cnh"
    print(f"[{label}] 正在从 Investing.com 获取...")

    try:
        url = f"https://cn.investing.com/currencies/{slug}-historical-data"
        custom_headers = {
            **HEADERS_COMMON,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Referer": "https://cn.investing.com/",
        }
        resp = requests.get(url, headers=custom_headers, timeout=15)

        if resp.status_code != 200:
            return None, None, f"Investing HTTP {resp.status_code}"

        html = resp.text

        # 从页面源码中提取 historicalDataStore 的 data 数组
        # 找到第一条记录（最新日期）
        # 精确提取 last_close 字段

        # 提取整个 data 数组内容
        pattern = r'"historicalData"\s*:\s*\{"data"\s*:\s*\[(.*?)\]\s*\}'
        match = re.search(pattern, html, re.DOTALL)

        if match:
            data_str = match.group(1)

            # 找到第一个 { } 对象（最新一天的数据）
            first_obj_pattern = r'\{(.*?)\}'
            first_obj_match = re.search(first_obj_pattern, data_str, re.DOTALL)

            if first_obj_match:
                obj_str = first_obj_match.group(1)
                print(f"[{label}] 调试 - 第一条记录内容片段: {obj_str[:300]}")

                # 精确提取 rowDate
                date_match = re.search(r'"rowDate"\s*:\s*"([^"]+)"', obj_str)
                date_text = date_match.group(1) if date_match else "未知日期"

                # 精确提取 last_close（字符串格式的收盘价）
                close_match = re.search(r'"last_close"\s*:\s*"([\d.]+)"', obj_str)

                if close_match:
                    rate = float(close_match.group(1))
                    if validate_rate(rate):
                        print(f"[{label}] ✅ Investing获取成功: {rate} (日期:{date_text})")
                        return rate, f"Investing.com({date_text})", None
                    else:
                        print(f"[{label}] 校验失败: {rate}")

                # 如果 last_close 没找到，试 last_closeRaw
                raw_match = re.search(r'"last_closeRaw"\s*:\s*([\d.]+)', obj_str)
                if raw_match:
                    rate = round(float(raw_match.group(1)), 4)
                    if validate_rate(rate):
                        print(f"[{label}] ✅ Investing(Raw)获取成功: {rate} (日期:{date_text})")
                        return rate, f"Investing.com({date_text})", None

        # 如果 historicalDataStore 没找到，尝试表格
        soup = BeautifulSoup(html, "lxml")
        tables = soup.find_all("table")
        for table in tables:
            header_text = table.get_text()
            if ("收盘" in header_text or "Close" in header_text) and \
               ("日期" in header_text or "Date" in header_text):
                rows = table.find_all("tr")
                for row in rows[1:]:
                    cells = row.find_all("td")
                    if len(cells) >= 2:
                        date_text = cells[0].get_text(strip=True)
                        close_text = cells[1].get_text(strip=True).replace(",", "")
                        try:
                            rate = float(close_text)
                            if validate_rate(rate):
                                print(f"[{label}] ✅ Investing(表格)获取成功: {rate} (日期:{date_text})")
                                return rate, f"Investing.com({date_text})", None
                        except ValueError:
                            continue

        return None, None, f"Investing页面未找到{label}数据"

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
            return None, None, "Yahoo Finance返回空数据"

        last_date = str(hist.index[-1].date())
        rate = round(float(hist.iloc[-1]["Close"]), 4)

        if validate_rate(rate):
            print(f"[{label}] ✅ Yahoo获取成功: {rate} (日期:{last_date})")
            return rate, f"Yahoo Finance({last_date})", None
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

    # 2. 在岸 CNY
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

    # 3. 离岸 CNH
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
    """保存到文件备份"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.dirname(script_dir)
    data_dir = os.path.join(repo_root, "data")
    os.makedirs(data_dir, exist_ok=True)

    filepath = os.path.join(data_dir, "latest.json")
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"结果已保存到 {filepath}")

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


def post_to_feishu(result):
    """POST 数据到飞书工作流 Webhook"""
    if not FEISHU_WEBHOOK_URL:
        print("⚠️ 未配置 FEISHU_WEBHOOK_URL，跳过飞书推送")
        return

    # 组装来源文本
    source_parts = []
    if result["source_mid"]:
        source_parts.append("中间价:" + result["source_mid"])
    if result["source_cny"]:
        source_parts.append("CNY:" + result["source_cny"])
    if result["source_cnh"]:
        source_parts.append("CNH:" + result["source_cnh"])
    source_text = " ".join(source_parts)

    # 组装备注
    remark_parts = []
    if result["mid_rate"] is None:
        remark_parts.append("⚠️中间价获取失败")
    if result["onshore_cny"] is None:
        remark_parts.append("⚠️在岸CNY获取失败")
    if result["offshore_cnh"] is None:
        remark_parts.append("⚠️离岸CNH获取失败")
    if result["errors"]:
        remark_parts.append(result["errors"])
    remark_text = " ".join(remark_parts)

    payload = {
        "date": result["date"],
        "mid_rate": result["mid_rate"] if result["mid_rate"] is not None else 0,
        "onshore_cny": result["onshore_cny"] if result["onshore_cny"] is not None else 0,
        "offshore_cnh": result["offshore_cnh"] if result["offshore_cnh"] is not None else 0,
        "source_text": source_text,
        "remark_text": remark_text,
    }

    print(f"正在推送到飞书 Webhook...")
    print(f"  payload: {json.dumps(payload, ensure_ascii=False)}")

    try:
        resp = requests.post(
            FEISHU_WEBHOOK_URL,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=15,
        )
        print(f"  飞书响应: {resp.status_code} {resp.text[:200]}")
        if resp.status_code == 200:
            print("✅ 飞书推送成功")
        else:
            print(f"⚠️ 飞书推送异常: HTTP {resp.status_code}")
    except Exception as e:
        print(f"❌ 飞书推送失败: {str(e)}")


if __name__ == "__main__":
    result = collect_all()
    save_result(result)
    post_to_feishu(result)
