import os, sqlite3, json, math
p=os.environ["FALCO_SQLITE_PATH"]
con=sqlite3.connect(p)
cur=con.cursor()

print("DIAMOND_SHORTLIST_DAVIDSON (NO NEW ATTOM CALLS)")
print("RULES: county=Davidson, status=enriched, auction_readiness=GREEN, dts_days between 21-60, avm_low>=300k, avm_spread_pct<=0.18")

q = """
SELECT
  l.lead_key,
  l.address,
  l.county,
  l.dts_days,
  l.auction_readiness,
  l.falco_score_internal,
  a.avm_value,
  a.avm_low,
  a.avm_high,
  a.attom_raw_json,
  a.enriched_at
FROM leads l
JOIN attom_enrichments a ON a.lead_key=l.lead_key
WHERE
  LOWER(l.county) LIKE '%davidson%'
  AND a.status='enriched'
  AND l.auction_readiness='GREEN'
  AND l.dts_days IS NOT NULL
  AND l.dts_days BETWEEN 21 AND 60
  AND a.avm_low IS NOT NULL
  AND a.avm_low >= 300000
ORDER BY a.avm_low DESC
"""
rows = cur.execute(q).fetchall()

def spread_pct(lo, hi):
    if not lo or not hi or lo <= 0: return None
    return (hi - lo) / lo

out=[]
for r in rows:
    lk, addr, county, dts, ar, score, avm, lo, hi, raw, ts = r
    sp = spread_pct(lo,hi)
    if sp is None or sp > 0.18:
        continue
    # determine whether merged detail exists (for later comps/attributes)
    merged=False
    try:
        d=json.loads(raw) if isinstance(raw,str) else raw
        merged = isinstance(d,dict) and "detail" in d and "avm" in d
    except Exception:
        merged=False
    out.append((lo, lk, dts, score, sp, merged, addr, ts))

print("CANDIDATES", len(out))
for lo, lk, dts, score, sp, merged, addr, ts in sorted(out, reverse=True)[:15]:
    print(" -", {"lead_key":lk,"avm_low":lo,"dts_days":dts,"score":score,"spread_pct":round(sp,4),"has_detail":merged,"address":addr,"enriched_at":ts})

con.close()
