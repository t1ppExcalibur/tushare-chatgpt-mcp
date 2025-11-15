import os
from typing import Any, Dict, List

import pandas as pd
import tushare as ts
from mcp.server.fastmcp import FastMCP

# 从环境变量读取 Tushare Token（后面在 Render 配）
TUSHARE_TOKEN = os.environ.get("TUSHARE_TOKEN")
if not TUSHARE_TOKEN:
    raise RuntimeError(
        "环境变量 TUSHARE_TOKEN 未设置。"
        "请在部署平台（比如 Render）的环境变量里配置你的 Tushare Token。"
    )

# 配置 Tushare
ts.set_token(TUSHARE_TOKEN)
pro = ts.pro_api()  # 初始化 pro 接口 :contentReference[oaicite:2]{index=2}

# 创建 MCP 服务器实例
mcp = FastMCP("Tushare Simple Server")


def df_to_records(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """把 DataFrame 转成 list[dict]，方便 LLM 使用。"""
    return df.to_dict(orient="records")


@mcp.tool()
def search_stocks(keyword: str) -> List[Dict[str, Any]]:
    """
    模糊搜索股票（代码或名称中包含 keyword）。

    参数:
      keyword: 关键字，比如 "平安"、"新能源"、"银行"、"000001"

    返回:
      股票列表，每一项包含 ts_code, name, area, industry, list_date 等字段。
    """
    # 查询所有正常上市 A 股 :contentReference[oaicite:3]{index=3}
    df = pro.stock_basic(
        exchange="",
        list_status="L",
        fields="ts_code,symbol,name,area,industry,market,exchange,list_date"
    )

    # 在代码或名称中匹配
    mask = df["ts_code"].str.contains(keyword, case=False) | df["name"].str.contains(
        keyword, case=False
    )
    results = df[mask]

    # 只返回前 200 条，防止一次性太大
    results = results.head(200)

    return df_to_records(results)


@mcp.tool()
def stock_basic(ts_code: str) -> Dict[str, Any]:
    """
    查询单只股票的基础信息。

    参数:
      ts_code: Tushare 代码，比如 "000001.SZ"、"600519.SH"

    返回:
      一条字典，包含行业、地区、上市日期等。
    """
    df = pro.stock_basic(
        ts_code=ts_code,
        fields=(
            "ts_code,symbol,name,area,industry,fullname,market,exchange,"
            "list_date,list_status,is_hs"
        ),
    )

    if df.empty:
        raise ValueError(f"未找到股票: {ts_code}")

    return df_to_records(df)[0]


@mcp.tool()
def daily(ts_code: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
    """
    获取日线行情（前复权）。

    参数:
      ts_code: 股票代码，如 "000001.SZ"
      start_date: 开始日期，形如 "20230101"
      end_date: 结束日期，形如 "20231231"

    返回:
      每日行情列表，每一行包含日期、开收高低、成交量等字段。
    """
    df = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)  # :contentReference[oaicite:4]{index=4}

    if df.empty:
        raise ValueError("指定区间无数据，检查代码或日期是否正确。")

    # 按日期升序排序
    df = df.sort_values("trade_date")
    return df_to_records(df)


if __name__ == "__main__":
    # 在云平台上通常会提供 PORT 环境变量
    port = int(os.environ.get("PORT", "8000"))

    # 使用官方建议的 streamable-http 传输，在 /mcp 上提供 MCP 接口 :contentReference[oaicite:5]{index=5}
    mcp.run(transport="streamable-http", host="0.0.0.0", port=port)
