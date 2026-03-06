import os, sqlite3
p=os.environ["FALCO_SQLITE_PATH"]
con=sqlite3.connect(p)
cur=con.cursor()

print("ATTOM_ENRICHER ENV SNAPSHOT")
for k in [
  "FALCO_ATTOM_MAX_ENRICH",
  "FALCO_ATTOM_COUNTY_FILTER",
  "FALCO_ATTOM_MAX_CALLS",
  "FALCO_MAX_ATTOM_CALLS",
  "ATTOM_MAX_CALLS",
  "FALCO_ATTOM_HARD_CAP",
]:
    print(k, "=", os.environ.get(k))

# show config if stored in run_metadata last row
try:
    row = cur.execute("SELECT run_id, created_at, config_json FROM run_metadata ORDER BY created_at DESC LIMIT 1").fetchone()
    if row:
        run_id, created_at, cfg = row
        print("LAST_RUN_META", run_id, created_at)
        print("CONFIG_JSON", cfg)
except Exception as e:
    print("RUN_META_READ_ERROR", e)

con.close()
