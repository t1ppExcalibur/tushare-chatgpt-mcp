import os
import time
import logging
from typing import Any, Dict, List, Optional

import httpx
from mcp.server.fastmcp import FastMCP

# ---------- 日志配置 ----------
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("tushare_mcp")

# ---------- Tushare 基本配置 ----------
TUSHARE_API_URL = "http://api.tushare.pro"  # 官方 HTTP 接口地址
TUSHARE_TOKEN_ENV = "TUSHARE_TOKEN"
DEFAULT_TIMEOUT_SECONDS = 8.0  # 单次 Tushare 请求超时时间，可按需要调整

# 不在模块导入阶段强制校验 token，避免服务直接起不来
if not os.environ.get(TUSHARE_TOKEN_ENV):
    logger.warning(
        "环境变量 %s 未设置，所有 Tushare 工具将无法正常返回数据。",
        TUSHARE_TOKEN_ENV,
    )

# ---------- 创建 MCP 服务器 ----------
mcp = FastMCP("Tushare HTTP MCP Server")

# ---------- 工具级缓存（减少频繁大列表请求） ----------
_STOCK_BASIC_CACHE: Optional[List[Dict[str, Any]]] = None
_STOCK_BASIC_CACHE_TS: Optional[float] = None
_STOCK_BASIC_CACHE_TTL = 6 * 3600  # 6 小时刷新一次缓存
_STOCK_BASIC_FIELDS = "ts_code,symbol,name,area,industry,market,exchange,list_date"


async def _call_tushare(
    api_name: str,
    params: Optional[Dict[str, Any]] = None,
    fields: Optional[str] = None,
    token: Optional[str] = None,
) -> Dict[str, Any]:
    """
    统一封装 Tushare HTTP POST 调用，并做超时与错误处理。

    返回结构:
      {
        "error": str | None,
        "api_name": str,
        "fields": List[str] | None,
        "rows": List[Dict[str, Any]],
        "raw": Dict[str, Any]  # 原始返回，可选
      }
    """
    token_val = token or os.environ.get(TUSHARE_TOKEN_ENV)
    if not token_val:
        msg = f"Tushare token 未配置，请在环境变量 {TUSHARE_TOKEN_ENV} 中设置。"
        logger.error(msg)
        return {
            "error": msg,
            "api_name": api_name,
            "fields": None,
            "rows": [],
            "raw": None,
        }

    payload: Dict[str, Any] = {
        "api_name": api_name,
        "token": token_val,
        "params": params or {},
    }
    if fields:
        payload["fields"] = fields

    logger.info("[tushare] api=%s params=%s fields=%s", api_name, params, fields)

    try:
        async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT_SECONDS) as client:
            resp = await client.post(TUSHARE_API_URL, json=payload)
            resp.raise_for_status()
            data = resp.json()
    except httpx.TimeoutException:
        msg = f"Tushare 接口调用超时（>{DEFAULT_TIMEOUT_SECONDS}s），请缩短查询区间或稍后重试。"
        logger.exception("[tushare] api=%s 超时", api_name)
        return {
            "error": msg,
            "api_name": api_name,
            "fields": None,
            "rows": [],
            "raw": None,
        }
    except Exception as e:
        msg = f"Tushare 接口调用发生网络错误: {type(e).__name__}: {e}"
        logger.exception("[tushare] api=%s 网络/解析异常", api_name)
        return {
            "error": msg,
            "api_name": api_name,
            "fields": None,
            "rows": [],
            "raw": None,
        }

    # 业务码检查
    code = data.get("code")
    msg = data.get("msg")
    if code != 0:
        friendly = f"Tushare 返回错误 code={code}, msg={msg}"
        logger.error("[tushare] api=%s 业务错误: %s", api_name, friendly)
        return {
            "error": friendly,
            "api_name": api_name,
            "fields": None,
            "rows": [],
            "raw": data,
        }

    d = data.get("data") or {}
    fields_list: List[str] = d.get("fields", []) or []
    items: List[List[Any]] = d.get("items", []) or []

    rows: List[Dict[str, Any]] = [dict(zip(fields_list, row)) for row in items]

    logger.info(
        "[tushare] api=%s 返回成功 rows=%d fields=%d",
        api_name,
        len(rows),
        len(fields_list),
    )

    return {
        "error": None,
        "api_name": api_name,
        "fields": fields_list,
        "rows": rows,
        "raw": data,
    }


async def _get_stock_basic_all() -> Dict[str, Any]:
    """
    带缓存地获取全部正常上市股票列表，用于 search_stocks。
    """
    global _STOCK_BASIC_CACHE, _STOCK_BASIC_CACHE_TS

    now = time.time()
    if (
        _STOCK_BASIC_CACHE is not None
        and _STOCK_BASIC_CACHE_TS is not None
        and now - _STOCK_BASIC_CACHE_TS < _STOCK_BASIC_CACHE_TTL
    ):
        logger.info(
            "[stock_basic_cache] 使用缓存 rows=%d",
            len(_STOCK_BASIC_CACHE),
        )
        return {
            "error": None,
            "api_name": "stock_basic",
            "fields": _STOCK_BASIC_FIELDS.split(","),
            "rows": _STOCK_BASIC_CACHE,
            "raw": None,
        }

    logger.info("[stock_basic_cache] 缓存失效或不存在，调用 Tushare 刷新...")
    res = await _call_tushare(
        api_name="stock_basic",
        params={"exchange": "", "list_status": "L"},
        fields=_STOCK_BASIC_FIELDS,
    )

    if res["error"] is None:
        _STOCK_BASIC_CACHE = res["rows"]
        _STOCK_BASIC_CACHE_TS = now
        logger.info(
            "[stock_basic_cache] 缓存刷新完成 rows=%d", len(_STOCK_BASIC_CACHE)
        )
    else:
        logger.error(
            "[stock_basic_cache] 刷新失败: %s",
            res["error"],
        )

    return res


# ---------- MCP 工具定义 ----------


@mcp.tool()
async def ping() -> str:
    """
    健康检查：不访问 Tushare，只返回一行文本。
    用于确认 MCP 服务本身是否正常工作。
    """
    logger.info("[ping] called")
    return "MCP is alive"


@mcp.tool()
async def tushare_query(
    api_name: str,
    params: Optional[Dict[str, Any]] = None,
    fields: Optional[str] = None,
    token: Optional[str] = None,
) -> Dict[str, Any]:
    """
    通用 Tushare 查询工具。

    - api_name: Tushare 接口名，例如 'stock_basic', 'daily', 'trade_cal' 等。
    - params: 接口参数 dict，按官方文档填写。
    - fields: 逗号分隔的字段列表，例如 "ts_code,trade_date,open,high,low,close"。
    - token: 可选；不传则使用环境变量 TUSHARE_TOKEN。
    """
    return await _call_tushare(api_name, params=params, fields=fields, token=token)


@mcp.tool()
async def search_stocks(keyword: str) -> Dict[str, Any]:
    """
    模糊搜索股票（代码或名称中包含 keyword）。

    参数:
      keyword: 关键字，比如 "平安"、"新能源"、"银行"、"000001"

    返回:
      {
        "error": str | None,
        "rows": [ {ts_code, symbol, name, area, industry, market, exchange, list_date}, ... ]
      }
    """
    keyword = (keyword or "").strip()
    if not keyword:
        return {"error": "keyword 不能为空", "rows": []}

    res = await _get_stock_basic_all()
    if res["error"] is not None:
        # 直接把错误透传给上层，让 ChatGPT 能看到具体原因
        return {"error": res["error"], "rows": []}

    rows = res["rows"]
    # 大小写不敏感匹配
    kw_lower = keyword.lower()
    filtered = []
    for row in rows:
        ts_code = str(row.get("ts_code", ""))
        name = str(row.get("name", ""))
        if kw_lower in ts_code.lower() or kw_lower in name.lower():
            filtered.append(row)

    # 限制返回数量，防止一次性数据量过大
    MAX_ROWS = 200
    if len(filtered) > MAX_ROWS:
        filtered = filtered[:MAX_ROWS]

    logger.info(
        "[search_stocks] keyword=%s 命中=%d",
        keyword,
        len(filtered),
    )

    return {
        "error": None,
        "rows": filtered,
    }


@mcp.tool()
async def stock_basic(ts_code: str) -> Dict[str, Any]:
    """
    查询单只股票的基础信息。

    参数:
      ts_code: Tushare 代码，比如 "000001.SZ"、"600519.SH"

    返回:
      {
        "error": str | None,
        "row": { ... 字段 ... }
      }
    """
    ts_code = (ts_code or "").strip()
    if not ts_code:
        return {"error": "ts_code 不能为空", "row": None}

    res = await _call_tushare(
        api_name="stock_basic",
        params={"ts_code": ts_code},
        fields="ts_code,symbol,name,area,industry,fullname,market,exchange,"
        "list_date,list_status,is_hs",
    )

    if res["error"] is not None:
        return {"error": res["error"], "row": None}

    rows = res["rows"]
    if not rows:
        msg = f"未找到股票: {ts_code}"
        logger.warning("[stock_basic] %s", msg)
        return {"error": msg, "row": None}

    # 理论上只会有一行
    return {"error": None, "row": rows[0]}


@mcp.tool()
async def daily(
    ts_code: str,
    start_date: str,
    end_date: str,
) -> Dict[str, Any]:
    """
    获取日线行情。

    参数:
      ts_code: 股票代码，如 "000001.SZ"
      start_date: 开始日期，形如 "20240101"
      end_date: 结束日期，形如 "20251115"

    返回:
      {
        "error": str | None,
        "rows": [ {ts_code, trade_date, open, high, low, close, ...}, ... ]
      }
    """
    ts_code = (ts_code or "").strip()
    start_date = (start_date or "").strip()
    end_date = (end_date or "").strip()

    if not ts_code:
        return {"error": "ts_code 不能为空", "rows": []}
    if not start_date or not end_date:
        return {"error": "start_date 和 end_date 不能为空", "rows": []}

    fields = (
        "ts_code,trade_date,open,high,low,close,pre_close,change,"
        "pct_chg,vol,amount"
    )

    res = await _call_tushare(
        api_name="daily",
        params={"ts_code": ts_code, "start_date": start_date, "end_date": end_date},
        fields=fields,
    )

    if res["error"] is not None:
        return {"error": res["error"], "rows": []}

    rows = res["rows"]
    if not rows:
        msg = "指定区间无数据，检查代码或日期是否正确。"
        logger.warning("[daily] %s ts_code=%s %s-%s", msg, ts_code, start_date, end_date)
        return {"error": msg, "rows": []}

    # Tushare 返回的 trade_date 通常是降序，这里按日期升序排一下
    rows_sorted = sorted(rows, key=lambda r: r.get("trade_date", ""))

    logger.info(
        "[daily] ts_code=%s %s-%s rows=%d",
        ts_code,
        start_date,
        end_date,
        len(rows_sorted),
    )

    return {"error": None, "rows": rows_sorted}


# ---------- 暴露为 Streamable HTTP ASGI 应用 ----------
app = mcp.streamable_http_app()


if __name__ == "__main__":
    # Render / 其它云平台通常会通过 PORT 环境变量指定端口
    import uvicorn

    port = int(os.environ.get("PORT", "8000"))
    logger.info("Starting MCP server on 0.0.0.0:%d", port)
    uvicorn.run(app, host="0.0.0.0", port=port)
