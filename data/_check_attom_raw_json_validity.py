import os, sqlite3, json
p=os.environ["FALCO_SQLITE_PATH"]
con=sqlite3.connect(p)
cur=con.cursor()

# pull the most recent 25 rows
rows = cur.execute("""
SELECT id, lead_key, status, enriched_at, attom_raw_json
FROM attom_enrichments
ORDER BY id DESC
LIMIT 25
""").fetchall()

print("CHECK_LAST_25_ATTOM_ROWS_JSON")
bad = 0
good = 0
none = 0

for _id, lk, st, ts, raw in rows:
    if raw is None:
        none += 1
        continue
    if not isinstance(raw, str):
        # sqlite usually returns str; if not, treat as bad for now
        bad += 1
        print(" - BAD_TYPE", _id, lk, st, ts, type(raw).__name__)
        continue
    try:
        d = json.loads(raw)
        good += 1
        # show keys to confirm merged format
        if isinstance(d, dict):
            print(" - OK", _id, lk, st, ts, "keys=", list(d.keys())[:10])
        else:
            print(" - OK_NONDICT", _id, lk, st, ts, "type=", type(d).__name__)
    except Exception as e:
        bad += 1
        preview = raw[:120].replace("\n"," ")
        print(" - BAD_JSON", _id, lk, st, ts, "preview=", preview)

print("SUMMARY", {"good":good,"bad":bad,"none":none})

con.close()
