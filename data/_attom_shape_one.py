import os, sqlite3, json
p=os.environ["FALCO_SQLITE_PATH"]
con=sqlite3.connect(p)
cur=con.cursor()

lead_key = "db809279ca102ef3e87be7d098c45f42da09d96c"  # Scarcroft (highest AVM)
raw = cur.execute("SELECT attom_raw_json FROM attom_enrichments WHERE lead_key=? ORDER BY id DESC LIMIT 1", (lead_key,)).fetchone()[0]
data = json.loads(raw) if isinstance(raw,str) else raw

print("TOP_KEYS", list(data.keys())[:50])

# Try common paths and print what exists
paths = [
  ["property"],
  ["property","detail"],
  ["property","mortgage"],
  ["mortgage"],
  ["deed"],
  ["sale"],
  ["assessment"],
  ["avm"],
  ["response"],
  ["property","building"],
  ["property","summary"],
]
def get_path(d, path):
    o=d
    for k in path:
        if isinstance(o, dict) and k in o:
            o=o[k]
        else:
            return None
    return o

print("PATH_SAMPLES")
for path in paths:
    o=get_path(data,path)
    if o is None:
        continue
    if isinstance(o, dict):
        print(" -", ".".join(path), "DICT_KEYS", list(o.keys())[:50])
    elif isinstance(o, list):
        print(" -", ".".join(path), "LIST_LEN", len(o), "FIRST_TYPE", type(o[0]).__name__ if o else None)
    else:
        print(" -", ".".join(path), "VAL", o)

con.close()
