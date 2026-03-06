import os, sqlite3, json
p=os.environ["FALCO_SQLITE_PATH"]
con=sqlite3.connect(p)
cur=con.cursor()

row = cur.execute("""
SELECT a.id, l.lead_key, l.address, a.status, a.enriched_at, a.attom_raw_json
FROM attom_enrichments a
JOIN leads l ON l.lead_key = a.lead_key
WHERE LOWER(l.county) LIKE '%davidson%'
ORDER BY a.id DESC
LIMIT 1
""").fetchone()

print("LATEST_DAVIDSON_ATTOM", row[0], row[1], row[2], row[3], row[4])

raw = row[5]
d = json.loads(raw) if isinstance(raw,str) else raw

print("RAW_TOP_KEYS", list(d.keys()) if isinstance(d,dict) else type(d).__name__)
print("AVM_PRESENT", isinstance(d,dict) and "avm" in d)
print("DETAIL_PRESENT", isinstance(d,dict) and "detail" in d)

if isinstance(d,dict) and isinstance(d.get("avm"), dict):
    print("AVM_KEYS", list(d["avm"].keys())[:20])
if isinstance(d,dict) and isinstance(d.get("detail"), dict):
    print("DETAIL_KEYS", list(d["detail"].keys())[:30])

con.close()
