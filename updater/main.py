import os, json, time
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd
from fredapi import Fred
import yfinance as yf
import feedparser

OUT = 'data/state.json'

fred = Fred(api_key=os.environ['FRED_API_KEY'])

# ------------ helpers ------------
def latest_two(s: pd.Series):
  s = s.dropna()
  if len(s) < 2: return (None, None, None)
  return s.index[-1], float(s.iloc[-1]), float(s.iloc[-2])

def pct(a, b):
  if b is None or b == 0 or a is None: return None
  return (a / b) - 1.0

def month_name(dt_idx):
  return dt_idx.strftime('%b') if hasattr(dt_idx, 'strftime') else None

def ytd_stats(series: pd.Series, last_date):
  if last_date is None or pd.isna(last_date): return (None, None, None, None)
  Y, M = last_date.year, last_date.month
  this_ytd = series[(series.index.year==Y) & (series.index.month<=M)].sum()
  prev_ytd = series[(series.index.year==Y-1) & (series.index.month<=M)].sum()
  yoy = None if (prev_ytd in (None, 0)) else (this_ytd/prev_ytd - 1.0)
  return int(this_ytd), yoy, Y, month_name(pd.Timestamp(last_date))

# ------------ macro ------------
def get_spx():
  df = yf.download("^GSPC", period="10d", interval="1d", auto_adjust=False, progress=False)
  if df.empty: return None
  close = df['Close'].dropna()
  if len(close) < 2: return None
  last, prev = float(close.iloc[-1]), float(close.iloc[-2])
  return {"price": round(last, 2), "pct_1d": (last/prev - 1.0)}

def get_10y():
  s = fred.get_series('DGS10')  # percent, daily
  dt, last, prev = latest_two(s)
  return {"yield_pct": last, "delta_bp_1d": (last - prev)*100.0}

def get_mort30():
  s = fred.get_series('MORTGAGE30US')  # percent, weekly
  dt, last, prev = latest_two(s)
  return {"rate_pct": last, "delta_bp_wow": (last - prev)*100.0}

def get_starts():
  s = fred.get_series('HOUST')  # SAAR units, monthly
  dt, last, prev = latest_two(s)
  return {"level_saar": last, "mom": pct(last, prev)}

# ------------ permits: FL + MSAs + DOM ------------
def permits_state_florida():
  s = fred.get_series('FLBPPRIV')  # NSA units, monthly
  dt, last, prev = latest_two(s)
  prev12 = s.shift(12).reindex([dt]).iloc[0] if dt in s.index else None
  ytd_count, ytd_yoy, yr, mname = ytd_stats(s, dt)
  return {
    "latest": int(last) if last is not None else None,
    "latest_date": dt.strftime('%Y-%m') if dt else None,
    "mom": pct(last, prev),
    "yoy": pct(last, prev12),
    "ytd": ytd_yoy,
    "ytd_count": ytd_count,
    "latest_year": yr, "latest_month_name": mname
  }

MSA_SERIES = {
  "Orlando–Kissimmee–Sanford": {
    "total": "ORLA712BPPRIV", "one_unit": "ORLA712BP1FH", "dom": "MEDDAYONMAR36740"
  },
  "Tampa–St. Petersburg–Clearwater": {
    "total": "TAMP312BPPRIV", "one_unit": "TAMP312BP1FH", "dom": "MEDDAYONMAR45300"
  },
  "Ocala": {
    "total": "OCAL112BPPRIV", "one_unit": "OCAL112BP1FH", "dom": "MEDDAYONMAR36100"
  },
  "Palm Bay–Melbourne–Titusville": {
    "total": "PALM312BPPRIV", "one_unit": "PALM312BP1FH", "dom": "MEDDAYONMAR37340"
  },
}

def metro_bucket(series_id):
  s = fred.get_series(series_id)
  dt, last, prev = latest_two(s)
  prev12 = s.shift(12).reindex([dt]).iloc[0] if dt in s.index else None
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
  if s.empty: return None
  dt = s.index[-1]; last = float(s.iloc[-1])
  prev12 = s.shift(12).reindex([dt]).iloc[0] if dt in s.index else None
  return {"days": int(round(last)), "yoy": pct(last, prev12), "as_of": dt.strftime('%Y-%m')}

def permits_msas():
  out = {}
  for name, ids in MSA_SERIES.items():
    total = metro_bucket(ids["total"]) if ids.get("total") else None
    one = metro_bucket(ids["one_unit"]) if ids.get("one_unit") else None
    d = metro_dom(ids["dom"]) if ids.get("dom") else None
    out[name] = {"total": total, "one_unit": one, "dom": d}
  return out

# ------------ watchlist (prices + % change windows) ------------
WATCHLIST = [
  "DHI","LEN","NVR","TOL","PHM","KBH","MTH","GRBK","BLDR","HD","LOW",
  "XHB","ITB","VNQ","SPG","PLD","AMT","MAA","AVB","EQR","O","INVH","AMH",
  "Z","RDFN","OPEN","OPAD","RKT","UWMC","BAC","WFC","JPM","USB"
]

def get_watchlist():
  out = []
  # pull 3 months of daily to compute 1D/WTD/MTD/YTD simple % changes
  tickers = " ".join(WATCHLIST)
  df = yf.download(tickers, period="3mo", interval="1d", auto_adjust=False, progress=False, group_by='ticker', threads=True)
  today = pd.Timestamp.utcnow().normalize()
  # helper to locate period opens
  def period_anchor(ts: pd.Timestamp, kind: str):
    if kind=="W":  # Monday open of current ISO week
      d = ts - pd.to_timedelta(ts.weekday(), unit='D')
      return d
    if kind=="M":
      return ts.replace(day=1)
    if kind=="Y":
      return ts.replace(month=1, day=1)
    return ts

  for t in WATCHLIST:
    try:
      sub = df[t]['Close'].dropna()
      if sub.empty: continue
      last_date = sub.index[-1]
      last = float(sub.iloc[-1])
      prev = float(sub.iloc[-2]) if len(sub)>=2 else None

      # anchors (use index dates present)
      w_anchor = period_anchor(last_date, "W")
      m_anchor = period_anchor(last_date, "M")
      y_anchor = period_anchor(last_date, "Y")
      def anchor_val(anchor):
        # pick the first close ON or AFTER the anchor within available data
        s = sub[sub.index >= anchor]
        return float(s.iloc[0]) if len(s)>0 else None

      v_w = anchor_val(w_anchor)
      v_m = anchor_val(m_anchor)
      v_y = anchor_val(y_anchor)

      out.append({
        "symbol": t,
        "price": round(last, 2),
        "pct_1d": None if prev is None else (last/prev - 1.0),
        "pct_wtd": None if v_w in (None,0) else (last/v_w - 1.0),
        "pct_mtd": None if v_m in (None,0) else (last/v_m - 1.0),
        "pct_ytd": None if v_y in (None,0) else (last/v_y - 1.0),
      })
    except Exception as e:
      # keep going; one bad ticker shouldn't kill the run
      continue
  return out

# ------------ FEED (simple RSS pull) ------------
RSS_SOURCES = [
  "https://www.housingwire.com/feed/",
  "https://www.redfin.com/news/feed/",
  "https://therealdeal.com/feed/",
]
MAX_ITEMS = 12

def get_feed():
  items = []
  for url in RSS_SOURCES:
    try:
      feed = feedparser.parse(url)
      for e in feed.entries[:6]:
        items.append({
          "title": e.get("title", "").strip(),
          "link": e.get("link", "").strip(),
          "published": e.get("published", "")[:16]  # short
        })
    except Exception:
      continue
  # de-dup by title
  seen = set()
  dedup = []
  for it in items:
    k = it["title"]
    if k and k not in seen:
      seen.add(k)
      dedup.append(it)
  return dedup[:MAX_ITEMS]

# ------------ 10/10/20 AUTO-FEED (additive; does not alter existing feed) ------------
# Builds S['feed_101020'] with three buckets from RSS: macro, housing, notables.
# Each item has: title, url, what, numbers, justified, why, market.
KEYWORDS = {
  "macro":   ["fed", "inflation", "cpi", "pce", "rates", "treasury", "yield", "mortgage rate", "jobs", "payrolls"],
  "housing": ["housing", "permit", "starts", "builder", "home", "mortgage", "realtor", "zillow", "redfin", "rent", "inventory"]
}
def _classify_bucket(title_lower: str):
  for k in KEYWORDS["macro"]:
    if k in title_lower: return "macro"
  for k in KEYWORDS["housing"]:
    if k in title_lower: return "housing"
  return "notables"

def build_101020_from_rss(max_macro=10, max_housing=10, max_notables=20):
  bucket = {"macro": [], "housing": [], "notables": []}
  try:
    pool = []
    for url in RSS_SOURCES:
      try:
        feed = feedparser.parse(url)
        for e in feed.entries[:12]:
          title = (e.get("title") or "").strip()
          link  = (e.get("link")  or "").strip()
          if not title: continue
          pool.append((title, link))
      except Exception:
        continue
    # de-dup by title (case-insensitive)
    seen = set()
    for title, link in pool:
      key = title.lower()
      if key in seen: continue
      seen.add(key)
      b = _classify_bucket(key)
      item = {
        "title": title,
        "url": link,
        # 10/10/20 shape expected by the dashboard:
        "what": title,
        "numbers": "",
        "justified": "Source: RSS",
        "why": "",
        "market": ""
      }
      bucket[b].append(item)
    bucket["macro"]   = bucket["macro"][:max_macro]
    bucket["housing"] = bucket["housing"][:max_housing]
    bucket["notables"]= bucket["notables"][:max_notables]
  except Exception:
    # never fail the run
    pass
  return bucket

# ------------ build + write ------------
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
    },
    "watchlist": get_watchlist(),
    "feed": get_feed(),
    # ADD: 10/10/20 feed (auto-built from RSS); safe & non-breaking
    "feed_101020": build_101020_from_rss(),
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
