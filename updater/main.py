import os, json
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
from fredapi import Fred
import yfinance as yf

OUT = 'data/state.json'

fred = Fred(api_key=os.environ['FRED_API_KEY'])

def latest_two(s: pd.Series):
    s = s.dropna()
    if len(s) < 2:
        return None
    return s.index[-1], float(s.iloc[-1]), float(s.iloc[-2])

def pct(a, b):
    if b is None or b == 0 or a is None:
        return None
    return (a / b) - 1.0

def month_name(dt_idx):
    return dt_idx.strftime('%b')

def ytd_stats(series: pd.Series, last_date):
    if pd.isna(last_date):
        return (None, None, None, None)
    Y, M = last_date.year, last_date.month
    this_ytd = series[(series.index.year == Y) & (series.index.month <= M)].sum()
    prev_ytd = series[(series.index.year == Y - 1) & (series.index.month <= M)].sum()
    yoy = None if prev_ytd in (None, 0) else (this_ytd / prev_ytd - 1.0)
    return int(this_ytd), yoy, Y, month_name(pd.Timestamp(last_date))

def get_spx():
    """
    Robust S&P fetch:
      1) Try Yahoo ^GSPC (index)
      2) Fallback to Yahoo SPY ETF (proxy)
      3) Fallback to FRED SP500 (daily close, may be 1-day lag)
    Returns {"price": float, "pct_1d": float} or None.
    """
    # 1) ^GSPC via yfinance
    try:
        df = yf.download("^GSPC", period="10d", interval="1d", auto_adjust=False, progress=False)
        close = df['Close'].dropna() if not df.empty else pd.Series(dtype=float)
        if not close.empty and len(close) >= 2:
            last = float(close.iloc[-1]); prev = float(close.iloc[-2])
            return {"price": round(last, 2), "pct_1d": (last / prev - 1.0)}
    except Exception:
        pass

    # 2) SPY proxy via yfinance
    try:
        df2 = yf.download("SPY", period="10d", interval="1d", auto_adjust=False, progress=False)
        close2 = df2['Close'].dropna() if not df2.empty else pd.Series(dtype=float)
        if not close2.empty and len(close2) >= 2:
            last = float(close2.iloc[-1]); prev = float(close2.iloc[-2])
            return {"price": round(last, 2), "pct_1d": (last / prev - 1.0)}
    except Exception:
        pass

    # 3) FRED SP500 (close), may be slightly lagged
    try:
        s = fred.get_series('SP500')
        s = s.dropna()
        if len(s) >= 2:
            last = float(s.iloc[-1]); prev = float(s.iloc[-2])
            return {"price": round(last, 2), "pct_1d": (last / prev - 1.0)}
    except Exception:
        pass

    return None  # all fallbacks failed

def get_10y():
    # percent, daily → also compute rounded bp delta
    s = fred.get_series('DGS10')
    dt, last, prev = latest_two(s)
    delta_bp = None if (last is None or prev is None) else round((last - prev) * 100.0)
    return {"yield_pct": last, "delta_bp_1d": delta_bp}

def get_mort30():
    # percent, weekly → rounded bp delta w/w
    s = fred.get_series('MORTGAGE30US')
    dt, last, prev = latest_two(s)
    delta_bp = None if (last is None or prev is None) else round((last - prev) * 100.0)
    return {"rate_pct": last, "delta_bp_wow": delta_bp}

def get_starts():
    # SAAR units, monthly → m/m %
    s = fred.get_series('HOUST')
    dt, last, prev = latest_two(s)
    return {"level_saar": last, "mom": pct(last, prev)}

def permits_state_florida():
    # NSA units, monthly
    s = fred.get_series('FLBPPRIV')
    dt, last, prev = latest_two(s)
    prev12 = s.shift(12).reindex([dt]).iloc[0] if (dt is not None and dt in s.index) else None
    yoy = pct(last, prev12)
    ytd_count, ytd_yoy, yr, mname = ytd_stats(s, dt)
    return {
        "latest": int(last) if last is not None else None,
        "latest_date": dt.strftime('%Y-%m') if dt else None,
        "mom": pct(last, prev),
        "yoy": yoy,
        "ytd": ytd_yoy,
        "ytd_count": ytd_count,
        "latest_year": yr,
        "latest_month_name": mname
    }

# Metro series IDs (NSA totals + 1-unit + DOM from Realtor.com via FRED)
MSA_SERIES = {
    "Orlando–Kissimmee–Sanford": {
        "total": "ORLA712BPPRIV",
        "one_unit": "ORLA712BP1FH",
        "dom": "MEDDAYONMAR36740"
    },
    "Tampa–St. Petersburg–Clearwater": {
        "total": "TAMP312BPPRIV",
        "one_unit": "TAMP312BP1FH",
        "dom": "MEDDAYONMAR45300"
    },
    "Ocala": {
        "total": "OCAL112BPPRIV",
        "one_unit": "OCAL112BP1FH",
        "dom": "MEDDAYONMAR36100"
    },
    "Palm Bay–Melbourne–Titusville": {
        "total": "PALM312BPPRIV",
        "one_unit": "PALM312BP1FH",
        "dom": "MEDDAYONMAR37340"
    },
}

def metro_bucket(series_id):
    s = fred.get_series(series_id)
    dt, last, prev = latest_two(s)
    prev12 = s.shift(12).reindex([dt]).iloc[0] if (dt is not None and dt in s.index) else None
    ytd_count, ytd_yoy, yr, mname = ytd_stats(s, dt)
    return {
        "latest": int(last) if last is not None else None,
        "latest_year": yr,
        "latest_month": (dt.month if dt else None),
        "latest_month_name": mname,
        "mom": pct(last, prev),
        "yoy": pct(last, prev12),
        "ytd_yoy": ytd_yoy,
        "ytd_count": ytd_count
    }

def metro_dom(series_id):
    s = fred.get_series(series_id)  # days (level), monthly
    s = s.dropna()
    if s.empty:
        return None
    dt = s.index[-1]; last = float(s.iloc[-1])
    prev12 = s.shift(12).reindex([dt]).iloc[0] if dt in s.index else None
    return {
        "days": int(round(last)),
        "yoy": pct(last, prev12),
        "as_of": dt.strftime('%Y-%m')
    }

def permits_msas():
    out = {}
    for name, ids in MSA_SERIES.items():
        total = metro_bucket(ids["total"]) if ids.get("total") else None
        one = metro_bucket(ids["one_unit"]) if ids.get("one_unit") else None
        d = metro_dom(ids["dom"]) if ids.get("dom") else None
        out[name] = {"total": total, "one_unit": one, "dom": d}
    return out

def build_state():
    state = {
        "as_of": datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC'),
        "macro": {
            "spx": get_spx(),
            "ten_year": get_10y(),
            "mortgage_30y": get_mort30(),
            "starts": get_starts(),
        },
        "permits": {
            "florida": permits_state_florida(),
            "msa": permits_msas()
        }
    }
    return state

def main():
    os.makedirs('data', exist_ok=True)
    state = build_state()
    with open(OUT, 'w', encoding='utf-8') as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    print(f"Wrote {OUT}")

if __name__ == '__main__':
    main()
