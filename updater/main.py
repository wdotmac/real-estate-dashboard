import os, json, time
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd
from fredapi import Fred
import yfinance as yf
import feedparser
from urllib.parse import urlparse  # <-- ADDED
import re, html  # <-- ADDED

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
# Builds S['feed_101020'] with three buckets from curated RSS: macro, housing, notables.
# Each item has: title, url, what, numbers, justified, why, market.

# ADDED: curated sources only for 10/10/20 (leave RSS_SOURCES for legacy FEED)
RSS_SOURCES_101020 = [
  "https://www.housingwire.com/feed/",
  "https://www.redfin.com/news/feed/",
  "https://therealdeal.com/feed/",
]

# ADDED: stricter keyword buckets
KEYWORDS_101020 = {
  "macro": [
    "fed","fomc","inflation","cpi","pce","jobs","payroll","unemployment",
    "treasury","yield","rates","mortgage rate","mortgage rates","bond","curve",
    "core","headline","powell","economic","gdp"
  ],
  "housing": [
    "housing","permit","permits","starts","completions","home sales","home prices",
    "mortgage","inventory","listings","new listings","dom","days on market",
    "pending","closed sales","builder","nahb","zillow","redfin","realtor.com","rent"
  ],
}
DAYS_WINDOW_101020 = 7  # ADDED: freshness window (days)

# keep your original (looser) KEYWORDS for anything else that might use it
KEYWORDS = {
  "macro":   ["fed", "inflation", "cpi", "pce", "rates", "treasury", "yield", "mortgage rate", "jobs", "payrolls"],
  "housing": ["housing", "permit", "starts", "builder", "home", "mortgage", "realtor", "zillow", "redfin", "rent", "inventory"]
}

# ADDED helpers for 10/10/20
def _ts_from_entry(e):
  for k in ("published_parsed","updated_parsed"):
    if getattr(e, k, None):
      return int(time.mktime(getattr(e, k)))
  return None

def _domain(u):
  try:
    return urlparse(u).netloc.replace("www.","")
  except Exception:
    return ""

def _match_bucket(title_lower: str, bucket: str):
  return any(k in title_lower for k in KEYWORDS_101020[bucket])

# ---------- helpers to structure 10/10/20 items ----------  # <-- ADDED
def _clean(txt: str) -> str:
  if not txt: return ""
  txt = re.sub(r"<[^>]+>", " ", str(txt))
  txt = html.unescape(txt)
  return re.sub(r"\s+", " ", txt).strip()

def _first_sentence(text: str) -> str:
  text = _clean(text)
  for sep in [". ", " — ", " – ", " | ", " • ", "! ", "? "]:
    if sep in text:
      return text.split(sep)[0].strip()
  return text[:180].strip()

_NUM_PAT = re.compile(
  r"(\$?\d[\d,\.]*\s?(?:billion|million|thousand|bn|mn|k)?|\d{1,3}(?:\.\d+)?%|\d+\s?bp|\$\d[\d,\.]*)",
  re.I
)

def _extract_numbers(text: str) -> str:
  text = _clean(text)
  seen = set(); out = []
  for m in _NUM_PAT.findall(text):
    t = m.strip()
    key = t.lower()
    if key in seen:
      continue
    seen.add(key); out.append(t)
  return ", ".join(out[:6])

def _infer_why_market(title_lc: str, body_lc: str):
  t = f"{title_lc} {body_lc}"
  if any(k in t for k in ["mortgage rate","mortgage rates","mortgage","rate","yield","treasury","curve","fed","fomc","cpi","pce","inflation"]):
    why = "Rates & inflation shape discount rates and housing affordability."
    market = "Lower rates support demand/valuations; higher rates pressure builders/REITs and slow transactions."
  elif any(k in t for k in ["permit","permits","starts","completions","builder","nahb"]):
    why = "Supply pipeline sets future inventory and construction activity."
    market = "Stronger pipeline aids suppliers; oversupply later can weigh on pricing."
  elif any(k in t for k in ["price","prices","case-shiller","hpi","home prices"]):
    why = "Price trend signals wealth effects and buyer/seller power."
    market = "Firm prices help builder margins; softness hits comps and sentiment."
  elif any(k in t for k in ["inventory","listing","listings","dom","days on market","pending","closed sales","transactions"]):
    why = "Inventory and velocity drive pricing power and volume."
    market = "Rising inventory/DOM tilts to buyers; tight supply supports prices."
  elif any(k in t for k in ["rent","vacancy"]):
    why = "Rent moves feed shelter inflation and multifamily fundamentals."
    market = "Rising rents help MF REITs; falling rents ease inflation but pressure NOI."
  else:
    why = "Relevant development for housing and markets."
    market = "Watch second-order effects across builders, lenders, and REITs."
  return why, market

def _entry_body(e):
  if "summary" in e: return str(e.summary)
  if "description" in e: return str(e.description)
  if "content" in e and e.content:
    try: return str(e.content[0].value)
    except Exception: pass
  return ""

# REPLACED: stricter builder for feed_101020 (now structured)  # <-- REPLACED
def build_101020_from_rss(max_macro=10, max_housing=10, max_notables=20):
  now_ts = int(time.time())
  cutoff = now_ts - DAYS_WINDOW_101020*24*3600

  pool = []
  out = {"macro": [], "housing": [], "notables": []}

  # fetch
  for url in RSS_SOURCES_101020:
    try:
      feed = feedparser.parse(url)
      for e in feed.entries[:25]:
        title = (e.get("title") or "").strip()
        link  = (e.get("link")  or "").strip()
        if not title or not link:
          continue
        ts = _ts_from_entry(e)
        if ts is None or ts < cutoff:
          continue
        pool.append((ts, title, link, e))
    except Exception:
      continue

  # de-dup by lowercase title, keep newest
  seen = {}
  for ts, title, link, e in pool:
    k = title.lower()
    if k not in seen or ts > seen[k][0]:
      seen[k] = (ts, title, link, e)

  # newest first
  items = sorted(seen.values(), key=lambda x: x[0], reverse=True)

  # classify + structure
  for ts, title, link, e in items:
    tl = title.lower()
    body_raw = _entry_body(e)
    body_lc  = _clean(body_raw).lower()

    if any(k in tl or k in body_lc for k in KEYWORDS_101020["macro"]):
      bucket = "macro"
    elif any(k in tl or k in body_lc for k in KEYWORDS_101020["housing"]):
      bucket = "housing"
    else:
      bucket = "notables"

    what     = _first_sentence(body_raw or title) or title
    numbers  = _extract_numbers(body_raw or title)
    why, mkt = _infer_why_market(tl, body_lc)
    just     = f"{_domain(link)} — {link}"

    out[bucket].append({
      "title": title,
      "url": link,
      "what": what,
      "numbers": numbers,
      "justified": just,
      "why": why,
      "market": mkt
    })

  # trim to 10/10/20
  out["macro"]   = out["macro"][:max_macro]
  out["housing"] = out["housing"][:max_housing]
  out["notables"]= out["notables"][:max_notables]

  # clear action log
  print(f"[feeds] 10/10/20 built from RSS: macro={len(out['macro'])}, housing={len(out['housing'])}, notables={len(out['notables'])}")

  return out

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

  # NEW: print feed_101020 counts to the GitHub Actions log
  try:
    f = state.get("feed_101020", {})
    print(
      f"[feeds] 10/10/20 built from RSS: "
      f"macro={len(f.get('macro', []))}, "
      f"housing={len(f.get('housing', []))}, "
      f"notables={len(f.get('notables', []))}"
    )
  except Exception as e:
    print(f"[feeds] error: {e}")

  with open(OUT, 'w', encoding='utf-8') as fobj:
    json.dump(state, fobj, ensure_ascii=False, indent=2)
  print(f"Wrote {OUT}")

if __name__ == '__main__':
  main()
