from __future__ import annotations

import akshare as ak

from app import tdx_service

INDEX_ALIAS_TO_CODE = {
    "hs300": "000300",
    "沪深300": "000300",
    "csi300": "000300",
    "000300": "000300",
}


def normalize_index_code(index_code: str) -> str:
    value = str(index_code).strip()
    if not value:
        raise ValueError("index_code 不能为空")
    return INDEX_ALIAS_TO_CODE.get(value.lower(), value)


def fetch_index_constituent_codes(index_code: str = "000300") -> list[str]:
    normalized = normalize_index_code(index_code)
    df = ak.index_stock_cons_weight_csindex(symbol=normalized)
    if "成分券代码" not in df.columns:
        raise ValueError(f"{normalized} 指数成分数据缺少字段: 成分券代码")
    codes = [tdx_service.format_code(code) for code in df["成分券代码"].tolist()]
    return tdx_service.dedupe_keep_order(codes)
