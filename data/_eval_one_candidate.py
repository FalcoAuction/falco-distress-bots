import os, sqlite3, json
p=os.environ["FALCO_SQLITE_PATH"]
con=sqlite3.connect(p)
cur=con.cursor()

lk = "f07efd9f3071c2f84f5f6fc39cd339c90ff4d216"

row = cur.execute("""
SELECT
  l.lead_key,
  l.address,
  l.county,
  l.dts_days,
  l.auction_readiness,
  l.falco_score_internal,
  a.status,
  a.avm_value,
  a.avm_low,
  a.avm_high,
  a.attom_raw_json,
  a.enriched_at
FROM leads l
JOIN attom_enrichments a ON a.lead_key=l.lead_key
WHERE l.lead_key=?
ORDER BY a.id DESC
LIMIT 1
""", (lk,)).fetchone()

print("LEAD_ROW", row[:10], row[11])

status=row[6]; avm=row[7]; lo=row[8]; hi=row[9]; raw=row[10]
spread = None
if lo and hi and lo>0:
    spread = (hi-lo)/lo

merged=False
try:
    d=json.loads(raw) if isinstance(raw,str) else raw
    merged = isinstance(d,dict) and "avm" in d and "detail" in d
except Exception:
    merged=False

print("METRICS", {"status":status,"avm":avm,"avm_low":lo,"avm_high":hi,"spread_pct":(round(spread,4) if spread is not None else None),"merged_detail":merged})

# Diamond proxy rules
diamond = (
    row[3] is not None and 21 <= row[3] <= 60 and
    row[4] == "GREEN" and
    status == "enriched" and
    lo is not None and lo >= 300000 and
    spread is not None and spread <= 0.18
)
print("DIAMOND_PROXY_PASS", diamond)

con.close()
