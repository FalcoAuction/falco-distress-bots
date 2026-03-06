import os, sqlite3, json
p=os.environ["FALCO_SQLITE_PATH"]
con=sqlite3.connect(p)
cur=con.cursor()

lead_key = "408587fa390f65481cd0b4d617185f6fc2432584"  # latest merged detail

raw = cur.execute("SELECT attom_raw_json, avm_value FROM attom_enrichments WHERE lead_key=? ORDER BY id DESC LIMIT 1", (lead_key,)).fetchone()
j, avm = raw
d = json.loads(j)

detail = d.get("detail") or {}
print("AVM_VALUE", avm)

# print top-level keys + likely debt/mortgage related subkeys
print("DETAIL_TOP_KEYS", list(detail.keys()))

candidates = ["mortgage","mortgageHistory","sale","deed","owner","assessment","tax","foreclosure","lien","loan","debt"]
for k in candidates:
    if k in detail:
        v = detail[k]
        if isinstance(v, dict):
            print("FOUND", k, "DICT_KEYS", list(v.keys())[:60])
        elif isinstance(v, list):
            print("FOUND", k, "LIST_LEN", len(v), "FIRST_TYPE", type(v[0]).__name__ if v else None)
        else:
            print("FOUND", k, "VAL_TYPE", type(v).__name__)

# also check if building/summary contains valuation / loan fields
for k in ["summary","assessment","tax","deed","sale"]:
    v = detail.get(k)
    if isinstance(v, dict):
        sub = [kk for kk in v.keys() if any(s in kk.lower() for s in ["loan","mort","lien","debt","amount","bal","equity","avm","value","assess","tax"])]
        if sub:
            print("SUBKEY_HINTS", k, sub[:60])

con.close()
