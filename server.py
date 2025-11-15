import os
from typing import Any, Dict, List

import pandas as pd
import tushare as ts
from mcp.server.fastmcp import FastMCP

# ========= 1. 初始化 Tushare =========

TUSHARE_TOKEN = os.environ.get("TUSHARE_TOKEN")
if not TUSHARE_TOKEN:
    raise RuntimeError(
        "环境变量 TUSHARE_TOKEN 未设置。"
        "请在 Render 的 Environment 里配置你的 Tushare token。"
    )

# 使用 Tushare 官方 Python SDK 初始化 pro 接口
ts.set_token(TUSHARE_TOKEN)
pro = ts.pro_api()

# ========= 2. 创建 MCP Server =========

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
    df = pro.stock_basic(
        exchange="",
        list_status="L",
        fields="ts_code,symbol,name,area,industry,market,exchange,list_date",
    )

    mask = df["ts_code"].str.contains(keyword, case=False) | df["name"].str.contains(
        keyword, case=False
    )
    results = df[mask].head(200)
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
    df = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)

    if df.empty:
        raise ValueError("指定区间无数据，检查代码或日期是否正确。")

    df = df.sort_values("trade_date")
    return df_to_records(df)


# ========= 3. 暴露为 Streamable HTTP ASGI 应用 =========
# 官方文档说明，Streamable HTTP 默认挂在 /mcp 路径下，适合作为远程 MCP endpoint。:contentReference[oaicite:5]{index=5}
app = mcp.streamable_http_app()


if __name__ == "__main__":
    # Render 要求绑定到 0.0.0.0 和 PORT 环境变量指定的端口:contentReference[oaicite:6]{index=6}
    import uvicorn

    port = int(os.environ.get("PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port)
