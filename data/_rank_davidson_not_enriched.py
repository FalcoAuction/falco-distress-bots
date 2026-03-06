import os, sqlite3
p=os.environ["FALCO_SQLITE_PATH"]
con=sqlite3.connect(p)
cur=con.cursor()

print("DAVIDSON TARGETS (NOT ENRICHED) — rank for lowest spend / highest diamond likelihood")
print("RULES: county=Davidson, dts_days 21-60, auction_readiness in (GREEN,YELLOW), prioritize GREEN, prioritize higher falco_score_internal, newest last_seen_at")

q = """
SELECT
  l.lead_key,
  l.address,
  l.county,
  l.dts_days,
  l.auction_readiness,
  l.falco_score_internal,
  l.last_seen_at
FROM leads l
LEFT JOIN attom_enrichments a ON a.lead_key=l.lead_key AND a.status='enriched'
WHERE
  LOWER(l.county) LIKE '%davidson%'
  AND a.lead_key IS NULL
  AND l.dts_days IS NOT NULL
  AND l.dts_days BETWEEN 21 AND 60
  AND l.auction_readiness IN ('GREEN','YELLOW')
ORDER BY
  CASE l.auction_readiness WHEN 'GREEN' THEN 0 ELSE 1 END,
  COALESCE(l.falco_score_internal,0) DESC,
  l.last_seen_at DESC
LIMIT 10
"""
rows = cur.execute(q).fetchall()
print("ROWS", len(rows))
for r in rows:
    print(" -", {"lead_key":r[0],"address":r[1],"dts_days":r[3],"readiness":r[4],"score":r[5],"last_seen_at":r[6]})

con.close()
