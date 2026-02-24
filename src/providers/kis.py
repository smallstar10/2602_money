from __future__ import annotations

import json
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from tenacity import retry, stop_after_attempt, wait_fixed

from src.core.config import Settings
from src.providers.base import DataProvider


class KisProvider(DataProvider):
    """KIS OpenAPI provider.

    Source:
    - koreainvestment/open-trading-api 공식 샘플
      - /uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice (FHKST03010200)
      - /uapi/domestic-stock/v1/quotations/inquire-daily-price (FHKST01010400)
      - /uapi/domestic-stock/v1/quotations/inquire-price (FHKST01010100)
      - /uapi/domestic-stock/v1/quotations/search-stock-info (CTPF1002R)
    """

    def __init__(self, settings: Settings):
        self.settings = settings
        self.base_url = "https://openapivts.koreainvestment.com:29443" if settings.kis_is_paper else "https://openapi.koreainvestment.com:9443"
        self._access_token: str | None = None
        self._token_expire_at: datetime | None = None
        self._session = requests.Session()
        self._sector_cache: dict[str, str] = {}
        self._token_cache_path = Path(settings.sqlite_path).parent / "kis_token_cache.json"

    def _check_credentials(self) -> None:
        if not self.settings.kis_app_key or not self.settings.kis_app_secret:
            raise RuntimeError("KIS_APP_KEY/KIS_APP_SECRET is required for DATA_PROVIDER=kis")

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(1), reraise=True)
    def _request(self, method: str, path: str, headers: dict[str, str] | None = None, params: dict[str, Any] | None = None, json_body: dict[str, Any] | None = None) -> dict[str, Any]:
        self._check_credentials()
        url = f"{self.base_url}{path}"
        merged = {"Content-Type": "application/json; charset=utf-8"}
        if headers:
            merged.update(headers)
        resp = self._session.request(method, url, headers=merged, params=params, json=json_body, timeout=15)
        if resp.status_code >= 400:
            body = ""
            try:
                body = str(resp.json())
            except Exception:
                body = resp.text[:400]
            raise RuntimeError(f"KIS HTTP {resp.status_code} {method} {path}: {body}")
        data = resp.json()
        rt_cd = str(data.get("rt_cd", "0"))
        if rt_cd not in ("0", ""):
            msg_cd = data.get("msg_cd", "")
            msg1 = data.get("msg1", "")
            raise RuntimeError(f"KIS API error {msg_cd}: {msg1}")
        return data

    def _get_access_token(self) -> str:
        if self._access_token and self._token_expire_at and datetime.utcnow() < self._token_expire_at:
            return self._access_token
        cached = self._load_cached_token(min_ttl_seconds=300)
        if cached:
            return cached
        payload = {
            "grant_type": "client_credentials",
            "appkey": self.settings.kis_app_key,
            "appsecret": self.settings.kis_app_secret,
        }
        try:
            data = self._request("POST", "/oauth2/tokenP", json_body=payload)
        except Exception as exc:
            if "EGW00133" in str(exc):
                # KIS token issuance limit: at most once per minute.
                cached_retry = self._load_cached_token(min_ttl_seconds=60)
                if cached_retry:
                    return cached_retry
                time.sleep(61)
                data = self._request("POST", "/oauth2/tokenP", json_body=payload)
            else:
                raise
        token = data.get("access_token", "")
        if not token:
            raise RuntimeError("failed to acquire KIS access token")
        expires_sec = int(data.get("expires_in", 23 * 3600))
        self._access_token = token
        self._token_expire_at = datetime.utcnow() + timedelta(seconds=max(300, expires_sec - 300))
        self._save_cached_token(self._access_token, self._token_expire_at)
        return token

    def _load_cached_token(self, min_ttl_seconds: int = 300) -> str | None:
        try:
            if not self._token_cache_path.exists():
                return None
            data = json.loads(self._token_cache_path.read_text(encoding="utf-8"))
            token = str(data.get("access_token", ""))
            expire_at = datetime.fromisoformat(str(data.get("expire_at_utc", "")))
            if not token:
                return None
            if datetime.utcnow() + timedelta(seconds=min_ttl_seconds) >= expire_at:
                return None
            self._access_token = token
            self._token_expire_at = expire_at
            return token
        except Exception:
            return None

    def _save_cached_token(self, token: str, expire_at_utc: datetime) -> None:
        try:
            self._token_cache_path.parent.mkdir(parents=True, exist_ok=True)
            self._token_cache_path.write_text(
                json.dumps(
                    {"access_token": token, "expire_at_utc": expire_at_utc.isoformat()},
                    ensure_ascii=True,
                ),
                encoding="utf-8",
            )
        except Exception:
            return

    def _headers(self, tr_id: str) -> dict[str, str]:
        token = self._get_access_token()
        return {
            "authorization": f"Bearer {token}",
            "appkey": self.settings.kis_app_key,
            "appsecret": self.settings.kis_app_secret,
            "tr_id": tr_id,
            "custtype": "P",
        }

    def _api_get(self, path: str, tr_id: str, params: dict[str, Any]) -> dict[str, Any]:
        return self._request("GET", path, headers=self._headers(tr_id), params=params)

    def _api_post(self, path: str, tr_id: str, body: dict[str, Any], use_hashkey: bool = False) -> dict[str, Any]:
        headers = self._headers(tr_id).copy()
        if use_hashkey:
            try:
                hashkey = self._issue_hashkey(body)
                if hashkey:
                    headers["hashkey"] = hashkey
            except Exception:
                pass
        return self._request("POST", path, headers=headers, json_body=body)

    def _issue_hashkey(self, payload: dict[str, Any]) -> str:
        data = self._request(
            "POST",
            "/uapi/hashkey",
            headers={
                "appkey": self.settings.kis_app_key,
                "appsecret": self.settings.kis_app_secret,
            },
            json_body=payload,
        )
        hashkey = str(data.get("HASH") or data.get("hash") or "").strip()
        return hashkey

    @staticmethod
    def _to_float(value: Any, default: float = 0.0) -> float:
        try:
            if value is None or value == "":
                return default
            return float(str(value).replace(",", ""))
        except Exception:
            return default

    @staticmethod
    def _first(d: dict[str, Any], keys: list[str], default: Any = None) -> Any:
        for k in keys:
            if k in d and d[k] not in ("", None):
                return d[k]
        return default

    def _fetch_daily(self, ticker: str) -> pd.DataFrame:
        data = self._api_get(
            "/uapi/domestic-stock/v1/quotations/inquire-daily-price",
            "FHKST01010400",
            {
                "FID_COND_MRKT_DIV_CODE": "J",
                "FID_INPUT_ISCD": ticker,
                "FID_PERIOD_DIV_CODE": "D",
                "FID_ORG_ADJ_PRC": "1",
            },
        )
        output = data.get("output", []) or []
        rows: list[dict[str, Any]] = []
        for item in output:
            ymd = self._first(item, ["stck_bsop_date", "xymd"])
            if not ymd:
                continue
            close = self._to_float(self._first(item, ["stck_clpr", "stck_prpr"]))
            volume = self._to_float(self._first(item, ["acml_vol", "cntg_vol"]))
            value = self._to_float(self._first(item, ["acml_tr_pbmn", "acml_trp"]))
            rows.append(
                {
                    "ticker": ticker,
                    "dt": pd.to_datetime(str(ymd), format="%Y%m%d", errors="coerce"),
                    "open": self._to_float(self._first(item, ["stck_oprc", "oprc"])),
                    "high": self._to_float(self._first(item, ["stck_hgpr", "hgpr"])),
                    "low": self._to_float(self._first(item, ["stck_lwpr", "lwpr"])),
                    "close": close,
                    "volume": volume,
                    "value": value if value > 0 else close * volume,
                }
            )
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows).dropna(subset=["dt"]).sort_values("dt")
        return df

    def _fetch_intraday(self, ticker: str) -> pd.DataFrame:
        now = datetime.now().strftime("%H%M%S")
        data = self._api_get(
            "/uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice",
            "FHKST03010200",
            {
                "FID_COND_MRKT_DIV_CODE": "J",
                "FID_INPUT_ISCD": ticker,
                "FID_INPUT_HOUR_1": now,
                "FID_PW_DATA_INCU_YN": "Y",
                "FID_ETC_CLS_CODE": "",
            },
        )
        output = data.get("output2", []) or []
        day = datetime.now().strftime("%Y%m%d")
        parsed: list[dict[str, Any]] = []
        for item in output:
            ymd = str(self._first(item, ["stck_bsop_date", "bsop_date"], day))
            hhmmss = str(self._first(item, ["stck_cntg_hour", "cntg_hour"], "000000")).zfill(6)
            dt = pd.to_datetime(f"{ymd}{hhmmss}", format="%Y%m%d%H%M%S", errors="coerce")
            px = self._to_float(self._first(item, ["stck_prpr", "stck_clpr", "prpr"]))
            op = self._to_float(self._first(item, ["stck_oprc", "oprc"]), px)
            hi = self._to_float(self._first(item, ["stck_hgpr", "hgpr"]), px)
            lo = self._to_float(self._first(item, ["stck_lwpr", "lwpr"]), px)
            vol = self._to_float(self._first(item, ["cntg_vol", "acml_vol", "acml_vol_yn"]))
            val = self._to_float(self._first(item, ["acml_tr_pbmn", "acml_trp"]))
            parsed.append({"dt": dt, "open": op, "high": hi, "low": lo, "close": px, "volume": vol, "value": val})

        if not parsed:
            return pd.DataFrame()

        minute = pd.DataFrame(parsed).dropna(subset=["dt"]).sort_values("dt")
        if minute.empty:
            return pd.DataFrame()

        if "value" in minute and minute["value"].max() > 0:
            diff_val = minute["value"].diff().fillna(minute["value"])
            minute["value"] = diff_val.clip(lower=0.0)
        else:
            minute["value"] = minute["close"] * minute["volume"]

        if "volume" in minute and minute["volume"].max() > 0 and minute["volume"].is_monotonic_increasing:
            diff_vol = minute["volume"].diff().fillna(minute["volume"])
            minute["volume"] = diff_vol.clip(lower=0.0)

        minute["hour"] = minute["dt"].dt.floor("h")
        agg = (
            minute.groupby("hour", as_index=False)
            .agg(open=("open", "first"), high=("high", "max"), low=("low", "min"), close=("close", "last"), volume=("volume", "sum"), value=("value", "sum"))
            .rename(columns={"hour": "dt"})
        )
        agg["ticker"] = ticker
        return agg[["ticker", "dt", "open", "high", "low", "close", "volume", "value"]]

    def _fetch_sector(self, ticker: str) -> str:
        if ticker in self._sector_cache:
            return self._sector_cache[ticker]
        try:
            data = self._api_get(
                "/uapi/domestic-stock/v1/quotations/search-stock-info",
                "CTPF1002R",
                {"PRDT_TYPE_CD": "300", "PDNO": ticker},
            )
            output = data.get("output", {})
            if isinstance(output, list):
                output = output[0] if output else {}
            sector = self._first(
                output,
                ["idx_bztp_scls_cd_name", "std_idst_clsf_cd_name", "idx_bztp_lcls_cd_name", "bstp_kor_isnm", "scts_name"],
                "UNKNOWN",
            )
            self._sector_cache[ticker] = str(sector)
        except Exception:
            self._sector_cache[ticker] = "UNKNOWN"
        return self._sector_cache[ticker]

    def _fetch_volume_rank_universe(self, market_code: str = "J", limit: int = 160) -> list[dict]:
        data = self._api_get(
            "/uapi/domestic-stock/v1/quotations/volume-rank",
            "FHPST01710000",
            {
                "FID_COND_MRKT_DIV_CODE": market_code,
                "FID_COND_SCR_DIV_CODE": "20171",
                "FID_INPUT_ISCD": "0000",
                "FID_DIV_CLS_CODE": "0",
                "FID_BLNG_CLS_CODE": "3",
                "FID_TRGT_CLS_CODE": "111111111",
                "FID_TRGT_EXLS_CLS_CODE": "0000000000",
                "FID_INPUT_PRICE_1": "0",
                "FID_INPUT_PRICE_2": "1000000",
                "FID_VOL_CNT": "0",
                "FID_INPUT_DATE_1": "",
            },
        )
        output = data.get("output", []) or []
        rows: list[dict] = []
        for item in output[:limit]:
            ticker = str(
                self._first(
                    item,
                    ["mksc_shrn_iscd", "stck_shrn_iscd", "pdno", "hts_kor_iscd"],
                    "",
                )
            )
            if not ticker:
                continue
            name = str(self._first(item, ["hts_kor_isnm", "prdt_name", "stck_name"], ticker))
            rows.append({"ticker": ticker, "name": name, "market": "KRX"})
        return rows

    def get_universe(self, universe_spec: str) -> list[dict]:
        rows = self._fetch_volume_rank_universe(market_code="J", limit=220)
        if rows:
            return rows
        raise RuntimeError("failed to build universe from KIS volume-rank API (empty response)")

    def get_latest_ohlcv(self, tickers: list[str], interval: str = "60m") -> pd.DataFrame:
        frames: list[pd.DataFrame] = []
        for ticker in tickers:
            try:
                if interval == "1d":
                    df = self._fetch_daily(ticker)
                else:
                    df = self._fetch_intraday(ticker)
                    if df.empty or len(df) < 5:
                        df = self._fetch_daily(ticker)
                if not df.empty:
                    frames.append(df)
            except Exception:
                continue
        if not frames:
            return pd.DataFrame(columns=["ticker", "dt", "open", "high", "low", "close", "volume", "value"])
        return pd.concat(frames, ignore_index=True)

    def get_investor_flow(self, tickers: list[str], window: int = 20) -> pd.DataFrame:
        scores: list[float] = []
        today = datetime.now().strftime("%Y%m%d")
        max_calls = 100
        for idx, ticker in enumerate(tickers):
            flow_score = 0.0
            try:
                if idx >= max_calls:
                    scores.append(0.0)
                    continue
                data = self._api_get(
                    "/uapi/domestic-stock/v1/quotations/investor-trade-by-stock-daily",
                    "FHPTJ04160001",
                    {
                        "FID_COND_MRKT_DIV_CODE": "J",
                        "FID_INPUT_ISCD": ticker,
                        "FID_INPUT_DATE_1": today,
                        "FID_ORG_ADJ_PRC": "",
                        "FID_ETC_CLS_CODE": "",
                    },
                )
                candidates = []
                for key in ("output1", "output2"):
                    part = data.get(key, [])
                    if isinstance(part, dict):
                        part = [part]
                    candidates.extend(part if isinstance(part, list) else [])

                for row in candidates:
                    frgn = self._to_float(self._first(row, ["frgn_ntby_qty", "frgn_seln_qty", "frgn_ntby_tr_pbmn"]))
                    orgn = self._to_float(self._first(row, ["orgn_ntby_qty", "orgn_seln_qty", "orgn_ntby_tr_pbmn"]))
                    prsn = self._to_float(self._first(row, ["prsn_ntby_qty", "prsn_seln_qty", "prsn_ntby_tr_pbmn"]))
                    flow_score += (frgn + orgn - prsn)
                flow_score = max(-1.0, min(1.0, flow_score / 1_000_000.0))
            except Exception:
                flow_score = 0.0
            scores.append(flow_score)
        return pd.DataFrame({"ticker": tickers, "flow_score": scores})

    def get_sector_map(self, tickers: list[str]) -> dict[str, str]:
        out: dict[str, str] = {}
        max_calls = 120
        for idx, ticker in enumerate(tickers):
            out[ticker] = self._fetch_sector(ticker) if idx < max_calls else "UNKNOWN"
        return out

    def _account_parts(self) -> tuple[str, str]:
        raw = str(self.settings.kis_account_no or "").strip()
        digits = "".join(ch for ch in raw if ch.isdigit())
        if len(digits) < 10:
            raise RuntimeError("KIS_ACCOUNT_NO 형식 오류: 계좌(8) + 상품코드(2)가 필요합니다.")
        return digits[:8], digits[8:10]

    def place_cash_order(
        self,
        *,
        ticker: str,
        qty: int,
        side: str,
        order_type: str = "01",
        price: float = 0.0,
    ) -> dict[str, Any]:
        cano, acnt = self._account_parts()
        side_u = str(side).upper()
        if side_u not in {"BUY", "SELL"}:
            raise RuntimeError(f"invalid side: {side}")
        qty_i = max(0, int(qty))
        if qty_i <= 0:
            raise RuntimeError("qty must be > 0")

        ord_unpr = "0" if float(price) <= 0 else str(int(round(float(price))))
        payload_new = {
            "CANO": cano,
            "ACNT_PRDT_CD": acnt,
            "PDNO": str(ticker),
            "ORD_DVSN": str(order_type),
            "ORD_QTY": str(qty_i),
            "ORD_UNPR": ord_unpr,
            "EXCG_ID_DVSN_CD": "KRX",
            "SLL_TYPE": "01",
            "CNDT_PRIC": "",
        }
        payload_old = {
            "CANO": cano,
            "ACNT_PRDT_CD": acnt,
            "PDNO": str(ticker),
            "ORD_DVSN": str(order_type),
            "ORD_QTY": str(qty_i),
            "ORD_UNPR": ord_unpr,
            "CTAC_TLNO": "",
            "SLL_TYPE": "01",
            "ALGO_NO": "",
        }

        if self.settings.kis_is_paper:
            tr_ids = ["VTTC0012U", "VTTC0802U"] if side_u == "BUY" else ["VTTC0011U", "VTTC0801U"]
        else:
            tr_ids = ["TTTC0012U", "TTTC0802U"] if side_u == "BUY" else ["TTTC0011U", "TTTC0801U"]

        last_exc: Exception | None = None
        for tr_id in tr_ids:
            for payload in (payload_new, payload_old):
                try:
                    data = self._api_post(
                        "/uapi/domestic-stock/v1/trading/order-cash",
                        tr_id,
                        payload,
                        use_hashkey=True,
                    )
                    output = data.get("output", {}) or {}
                    if isinstance(output, list):
                        output = output[0] if output else {}
                    order_no = str(self._first(output, ["ODNO", "odno", "order_no"], ""))
                    return {
                        "ok": True,
                        "tr_id": tr_id,
                        "order_no": order_no,
                        "output": output,
                        "raw": data,
                    }
                except Exception as exc:
                    last_exc = exc
                    continue
        raise RuntimeError(f"KIS 주문 실패({side_u} {ticker} x{qty_i}): {last_exc}")

    def inquire_buying_power(
        self,
        *,
        ticker: str,
        price: float,
        order_type: str = "01",
    ) -> dict[str, Any]:
        cano, acnt = self._account_parts()
        tr_id = "VTTC8908R" if self.settings.kis_is_paper else "TTTC8908R"
        ref_px = max(1, int(round(float(price or 0.0))))
        data = self._api_get(
            "/uapi/domestic-stock/v1/trading/inquire-psbl-order",
            tr_id,
            {
                "CANO": cano,
                "ACNT_PRDT_CD": acnt,
                "PDNO": str(ticker),
                "ORD_UNPR": str(ref_px),
                "ORD_DVSN": str(order_type),
                "CMA_EVLU_AMT_ICLD_YN": "N",
                "OVRS_ICLD_YN": "N",
            },
        )
        output = data.get("output", {}) or {}
        if isinstance(output, list):
            output = output[0] if output else {}

        nrcvb_buy_qty = int(self._to_float(self._first(output, ["nrcvb_buy_qty"], 0)))
        max_buy_qty = int(self._to_float(self._first(output, ["max_buy_qty"], 0)))
        nrcvb_buy_amt = self._to_float(self._first(output, ["nrcvb_buy_amt"], 0.0))
        max_buy_amt = self._to_float(self._first(output, ["max_buy_amt"], 0.0))
        ord_psbl_cash = self._to_float(self._first(output, ["ord_psbl_cash"], 0.0))
        unit_price = self._to_float(self._first(output, ["psbl_qty_calc_unpr"], float(ref_px)))

        return {
            "ticker": str(ticker),
            "unit_price": unit_price if unit_price > 0 else float(ref_px),
            "ord_psbl_cash": max(0.0, ord_psbl_cash),
            "nrcvb_buy_amt": max(0.0, nrcvb_buy_amt),
            "max_buy_amt": max(0.0, max_buy_amt),
            "nrcvb_buy_qty": max(0, nrcvb_buy_qty),
            "max_buy_qty": max(0, max_buy_qty),
            "raw": output,
        }

    def inquire_balance(self) -> dict[str, Any]:
        cano, acnt = self._account_parts()
        tr_id = "VTTC8434R" if self.settings.kis_is_paper else "TTTC8434R"
        data = self._api_get(
            "/uapi/domestic-stock/v1/trading/inquire-balance",
            tr_id,
            {
                "CANO": cano,
                "ACNT_PRDT_CD": acnt,
                "AFHR_FLPR_YN": "N",
                "OFL_YN": "",
                "INQR_DVSN": "01",
                "UNPR_DVSN": "01",
                "FUND_STTL_ICLD_YN": "N",
                "FNCG_AMT_AUTO_RDPT_YN": "N",
                "PRCS_DVSN": "00",
                "CTX_AREA_FK100": "",
                "CTX_AREA_NK100": "",
            },
        )

        output1 = data.get("output1", []) or []
        output2 = data.get("output2", []) or []
        if isinstance(output1, dict):
            output1 = [output1]
        if isinstance(output2, dict):
            output2 = [output2]

        positions: list[dict[str, Any]] = []
        for row in output1:
            ticker = str(self._first(row, ["pdno", "mksc_shrn_iscd"], ""))
            qty = int(self._to_float(self._first(row, ["hldg_qty", "hold_qty"], 0)))
            if not ticker or qty <= 0:
                continue
            avg = self._to_float(self._first(row, ["pchs_avg_pric", "pchs_avg_pric"], 0.0))
            last_px = self._to_float(self._first(row, ["prpr", "stck_prpr"], avg))
            eval_amt = self._to_float(self._first(row, ["evlu_amt", "evlu_amt2"], last_px * qty))
            pnl_amt = self._to_float(self._first(row, ["evlu_pfls_amt"], 0.0))
            pnl_pct_raw = self._to_float(self._first(row, ["evlu_pfls_rt"], 0.0))
            pnl_pct = pnl_pct_raw / 100.0 if abs(pnl_pct_raw) > 1.5 else pnl_pct_raw
            positions.append(
                {
                    "ticker": ticker,
                    "name": str(self._first(row, ["prdt_name", "hts_kor_isnm"], ticker)),
                    "qty": qty,
                    "avg_price": avg,
                    "last_price": last_px,
                    "eval_amount": eval_amt if eval_amt > 0 else last_px * qty,
                    "pnl_amount": pnl_amt,
                    "pnl_pct": pnl_pct,
                }
            )

        totals = output2[0] if output2 else {}
        # ord_psbl_cash: 주문가능현금(실제 주문 전 가능한 값)
        orderable_cash = self._to_float(self._first(totals, ["ord_psbl_cash", "dnca_tot_amt", "nxdy_excc_amt"], 0.0))
        deposit_cash = self._to_float(self._first(totals, ["dnca_tot_amt", "nxdy_excc_amt", "ord_psbl_cash"], 0.0))
        total_eval = self._to_float(self._first(totals, ["scts_evlu_amt", "tot_evlu_amt"], 0.0))
        total_asset = self._to_float(self._first(totals, ["tot_evlu_amt", "nass_amt"], 0.0))
        if total_asset <= 0:
            total_asset = orderable_cash + sum(float(p["eval_amount"]) for p in positions)

        return {
            "cash": orderable_cash,
            "deposit_cash": deposit_cash,
            "total_eval": total_eval if total_eval > 0 else sum(float(p["eval_amount"]) for p in positions),
            "total_asset": total_asset,
            "positions": positions,
        }
