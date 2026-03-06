import os, sqlite3
p=os.environ["FALCO_SQLITE_PATH"]
con=sqlite3.connect(p)
cur=con.cursor()

print("DAVIDSON ENRICHED CANDIDATES (top 20 by avm_value)")
q = """
SELECT
  l.lead_key,
  l.address,
  l.county,
  l.dts_days,
  l.auction_readiness,
  l.equity_band,
  l.falco_score_internal,
  a.status,
  a.avm_value,
  a.avm_low,
  a.avm_high
FROM leads l
JOIN attom_enrichments a ON a.lead_key = l.lead_key
WHERE
  (LOWER(l.county) LIKE '%davidson%')
  AND a.status = 'enriched'
ORDER BY a.avm_value DESC
LIMIT 20
"""
rows = cur.execute(q).fetchall()
print("ROWS", len(rows))
for r in rows:
    print(" -", r)

print("DAVIDSON NOT-ENRICHED (top 20 by last_seen_at)")
q2 = """
SELECT
  lead_key, address, county, dts_days, auction_readiness, equity_band, falco_score_internal, last_seen_at
FROM leads
WHERE (LOWER(county) LIKE '%davidson%')
ORDER BY last_seen_at DESC
LIMIT 20
"""
rows2 = cur.execute(q2).fetchall()
print("ROWS2", len(rows2))
for r in rows2:
    print(" -", r)

con.close()
