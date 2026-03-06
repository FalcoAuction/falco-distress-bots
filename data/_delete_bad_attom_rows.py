import os, sqlite3
p=os.environ["FALCO_SQLITE_PATH"]
con=sqlite3.connect(p)
cur=con.cursor()

bad_ids = [18,19,20]

print("BEFORE")
for i in bad_ids:
    row = cur.execute("SELECT id, lead_key, status, enriched_at, LENGTH(attom_raw_json) FROM attom_enrichments WHERE id=?", (i,)).fetchone()
    print(" -", row)

cur.executemany("DELETE FROM attom_enrichments WHERE id=?", [(i,) for i in bad_ids])
con.commit()

print("AFTER")
for i in bad_ids:
    row = cur.execute("SELECT id FROM attom_enrichments WHERE id=?", (i,)).fetchone()
    print(" -", i, "exists?", bool(row))

print("ATTOM_COUNT_NOW", cur.execute("SELECT COUNT(1) FROM attom_enrichments").fetchone()[0])

con.close()
