# updater/feeds.py
import re
import time
import socket
from urllib.parse import urlparse
from datetime import datetime, timezone
import feedparser
import yaml

NUM_RE = re.compile(r'(?:(?:\+|-)?\d{1,3}(?:,\d{3})*(?:\.\d+)?)\s*(?:%|bp|bps|b|bn|m|mm|k|thousand|million|billion)?', re.I)

def _domain(u):
    try:
        return urlparse(u).netloc.replace('www.', '')
    except Exception:
        return ''

def _numbers_from(text):
    if not text: return ''
    hits = NUM_RE.findall(text)
    out, seen = [], set()
    for h in hits:
        key = h.lower().replace(',', '')
        if key in seen: 
            continue
        seen.add(key)
        out.append(h)
    return ', '.join(out[:5])

def _classify(cfg, title, summary):
    t = f"{title or ''} {summary or ''}".lower()
    kw = cfg.get('keywords', {})
    def has_any(keys): 
        return any(k.lower() in t for k in (keys or []))
    if has_any(kw.get('housing')): return 'housing'
    if has_any(kw.get('macro')): return 'macro'
    if has_any(kw.get('notables')): return 'notables'
    if any(k in t for k in ['mortgage','housing','home','rent','permit','starts','case-shiller','fhfa','cmbs','reit']): 
        return 'housing'
    if any(k in t for k in ['fed','cpi','pce','fomc','treasury','10y','10-year','gdp','payroll']): 
        return 'macro'
    return 'notables'

def _clean_html(s):
    return re.sub(r'<[^>]+>', ' ', s or '').strip()

def load_config(path='updater/feeds.yml'):
    with open(path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f) or {}

def _parse_one(url, timeout=12):
    socket.setdefaulttimeout(timeout)
    d = feedparser.parse(url)
    if d.bozo:
        return []
    items = []
    for e in d.entries[:20]:
        title = (e.get('title') or '').strip()
        summary = _clean_html(e.get('summary') or e.get('description') or '')
        link = e.get('link') or ''
        published_ts = None
        for k in ['published_parsed','updated_parsed','created_parsed']:
            if e.get(k):
                published_ts = int(time.mktime(e.get(k)))
                break
        items.append({
            'title': title,
            'summary': summary,
            'link': link,
            'source': _domain(link) or _domain(url),
            'published_ts': published_ts or 0,
        })
    return items

def fetch_all(cfg):
    buckets = {'macro':[], 'housing':[], 'notables':[]}
    for sec, key in [('macro','macro_sources'), ('housing','housing_sources'), ('notables','notables_sources')]:
        for url in cfg.get(key, []) or []:
            try:
                for it in _parse_one(url):
                    cls = _classify(cfg, it['title'], it['summary'])
                    target = sec if sec in ('macro','housing','notables') else cls
                    if sec != 'notables' and cls != sec:
                        target = cls
                    buckets[target].append(it)
            except Exception as e:
                print(f"[feeds] skip {url}: {e}")
    for k in buckets:
        seen, out = set(), []
        for it in sorted(buckets[k], key=lambda x: x.get('published_ts',0), reverse=True):
            t = it['title'].strip().lower()
            if not t or t in seen: 
                continue
            seen.add(t)
            out.append(it)
        buckets[k] = out
    return buckets

def map_to_101020(buckets, max_macro=10, max_housing=10, max_notables=20):
    def to_items(arr, limit):
        out = []
        for it in arr[:limit]:
            title = it['title'] or '—'
            summary = it['summary'] or ''
            nums = _numbers_from(f"{title} {summary}")
            src = it.get('source') or 'news'
            link = it.get('link') or ''
            why = 'Macro relevance & policy path' if src in ('federalreserve.gov','bls.gov','bea.gov','census.gov') else 'Sector impact & positioning'
            market = 'Neutral → interpret via rates & breadth'
            out.append({
                'title': title,
                'what': summary[:240] + ('…' if len(summary)>240 else ''),
                'numbers': nums or '—',
                'justified_by': f"{src} — {link}",
                'why': why,
                'market_take': market
            })
        return out
    return {
        'macro': to_items(buckets.get('macro',[]), max_macro),
        'housing': to_items(buckets.get('housing',[]), max_housing),
        'notables': to_items(buckets.get('notables',[]), max_notables)
    }

def build_101020_from_rss(cfg_path='updater/feeds.yml', **limits):
    cfg = load_config(cfg_path)
    buckets = fetch_all(cfg)
    return map_to_101020(buckets, **limits)
