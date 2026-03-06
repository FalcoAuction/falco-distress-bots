import os, sqlite3, json
p=os.environ["FALCO_SQLITE_PATH"]
con=sqlite3.connect(p)
cur=con.cursor()

print("LATEST DAVIDSON ATTOM ROWS (merged avm+detail)")

q = """
SELECT
  l.lead_key,
  l.address,
  l.county,
  a.status,
  a.avm_value,
  a.avm_low,
  a.avm_high,
  a.enriched_at,
  a.attom_raw_json
FROM leads l
JOIN attom_enrichments a ON a.lead_key = l.lead_key
WHERE LOWER(l.county) LIKE '%davidson%'
ORDER BY a.id DESC
LIMIT 10
"""
rows = cur.execute(q).fetchall()
print("ROWS", len(rows))

def shape(raw):
    try:
        d=json.loads(raw) if isinstance(raw,str) else raw
    except Exception:
        return {"type":"unparseable"}
    if isinstance(d, dict) and "avm" in d and "detail" in d:
        return {"merged": True, "avm_type": type(d.get("avm")).__name__, "detail_type": type(d.get("detail")).__name__,
                "avm_keys": (list(d["avm"].keys())[:20] if isinstance(d.get("avm"), dict) else None),
                "detail_keys": (list(d["detail"].keys())[:20] if isinstance(d.get("detail"), dict) else None)}
    if isinstance(d, dict):
        return {"merged": False, "keys": list(d.keys())[:20]}
    return {"type": type(d).__name__}

for lead_key, addr, county, status, avm, low, high, enriched_at, raw in rows:
    print(" -", lead_key, "|", addr)
    print("   ", {"county":county,"status":status,"avm":avm,"low":low,"high":high,"enriched_at":enriched_at})
    print("   ", "RAW_SHAPE", shape(raw))

con.close()
