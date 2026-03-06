import os, sqlite3
p=os.environ["FALCO_SQLITE_PATH"]
con=sqlite3.connect(p)
cur=con.cursor()

cols=[r[1] for r in cur.execute("PRAGMA table_info(leads)").fetchall()]
print("LEADS_COLS", cols)

order_col = None
for c in ["last_seen_at","first_seen_at","score_updated_at","lead_key"]:
    if c in cols:
        order_col = c
        break

print("ORDER_BY", order_col)

print("leads SAMPLE (10)")
q = f"SELECT * FROM leads ORDER BY {order_col} DESC LIMIT 10"
rows=cur.execute(q).fetchall()
print("ROWS", len(rows))

for r in rows:
    d=dict(zip(cols,r))
    out={}
    for k in ["lead_key","address","county","state","first_seen_at","last_seen_at","dts_days","equity_band","auction_readiness","falco_score_internal","score_updated_at"]:
        if k in d: out[k]=d.get(k)
    print(" -", out)

con.close()
