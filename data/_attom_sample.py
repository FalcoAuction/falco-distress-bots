import os, sqlite3
p=os.environ["FALCO_SQLITE_PATH"]
con=sqlite3.connect(p)
cur=con.cursor()

cols=[r[1] for r in cur.execute("PRAGMA table_info(attom_enrichments)").fetchall()]
print("ATTOM_COLS", cols)

print("ATTOM SAMPLE (10)")
rows=cur.execute("SELECT * FROM attom_enrichments ORDER BY rowid DESC LIMIT 10").fetchall()
print("ROWS", len(rows))

for r in rows:
    d=dict(zip(cols,r))
    out={}
    for k in ["lead_key","address","county","state","created_at","status","match_quality","avm_value","avm_low","avm_high","confidence","detail_json","avm_json"]:
        if k in d: out[k]=d.get(k)
    print(" -", out)

con.close()
