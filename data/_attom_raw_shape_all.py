import os, sqlite3, json
p=os.environ["FALCO_SQLITE_PATH"]
con=sqlite3.connect(p)
cur=con.cursor()

keys = [
 "db809279ca102ef3e87be7d098c45f42da09d96c",
 "96f145db74a7ae78c285210eacdf642a9ce34699",
 "f6560628f9b5f7bfca7be631fc21656b3086c93e",
 "beb0bfd62d8d970c3e82e38c55c57c48bce95e2b",
 "7543a33d7b175ccaeb2cd6fc74716e3052c31150",
]

print("ATTOM_RAW_JSON SHAPE CHECK (key count + top keys)")
for k in keys:
    raw = cur.execute("SELECT attom_raw_json FROM attom_enrichments WHERE lead_key=? ORDER BY id DESC LIMIT 1", (k,)).fetchone()
    if not raw:
        print(" -", k, "NO_ROW")
        continue
    data = json.loads(raw[0]) if isinstance(raw[0], str) else raw[0]
    if isinstance(data, dict):
        print(" -", k, "DICT_KEYS_N", len(data.keys()), "TOP_KEYS", list(data.keys())[:30])
    elif isinstance(data, list):
        print(" -", k, "LIST_LEN", len(data))
    else:
        print(" -", k, "TYPE", type(data).__name__)

con.close()
