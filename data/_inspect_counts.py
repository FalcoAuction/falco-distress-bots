import os, sqlite3
p=os.environ["FALCO_SQLITE_PATH"]
con=sqlite3.connect(p)
cur=con.cursor()

tables = ["leads","attom_enrichments","stage2_gating_events","packets"]
for t in tables:
    try:
        n = cur.execute(f"SELECT COUNT(1) FROM {t}").fetchone()[0]
        print(f"{t} COUNT {n}")
    except Exception as e:
        print(f"{t} ERROR {e}")

print("leads SAMPLE (10)")
try:
    cols=[r[1] for r in cur.execute("PRAGMA table_info(leads)").fetchall()]
    print("LEADS_COLS", cols)
    rows=cur.execute("SELECT * FROM leads ORDER BY id DESC LIMIT 10").fetchall()
    print("ROWS", len(rows))
    for r in rows:
        d=dict(zip(cols,r))
        print(" -", {k:d.get(k) for k in ["id","source","county","address","city","state","zip","sale_date","created_at","updated_at"] if k in d})
except Exception as e:
    print("SAMPLE ERROR", e)

con.close()
