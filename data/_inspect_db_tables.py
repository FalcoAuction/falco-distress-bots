import os, sqlite3
p = os.environ.get("FALCO_SQLITE_PATH")
print("DB", p)
con = sqlite3.connect(p)
cur = con.cursor()
print("TABLES")
for (name,) in cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"):
    print(" -", name)
con.close()
