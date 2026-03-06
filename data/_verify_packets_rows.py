import os, sqlite3
p=os.environ["FALCO_SQLITE_PATH"]
con=sqlite3.connect(p)
cur=con.cursor()

keys = [
 "c3ddeeab29615dcc7a045f6e884b1f3e6b74ca08",
 "e1acd26d6ace91983665d7446579a31ea446bdaa",
 "db809279ca102ef3e87be7d098c45f42da09d96c",
 "706b83cc72c82e8c8b264d704eabe7789199bd1c",
 "beb0bfd62d8d970c3e82e38c55c57c48bce95e2b",
]

cols=[r[1] for r in cur.execute("PRAGMA table_info(packets)").fetchall()]
print("PACKETS_COLS", cols)

for k in keys:
    row = cur.execute("SELECT * FROM packets WHERE lead_key=? ORDER BY rowid DESC LIMIT 1", (k,)).fetchone()
    if not row:
        print(" -", k, "NO_PACKET_ROW")
        continue
    d=dict(zip(cols,row))
    print(" -", k, {kk:d.get(kk) for kk in cols})

con.close()
