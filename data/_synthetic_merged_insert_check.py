import os, sqlite3, json, datetime
p=os.environ["FALCO_SQLITE_PATH"]
con=sqlite3.connect(p)
cur=con.cursor()

lead_key = "TEST_MERGED_JSON_001"
merged = {
  "avm": {"eventDate":"2026-01-14","amount":{"value":123456,"low":111111,"high":135000,"scr":90}},
  "detail": {"property":{"address":{"oneLine":"123 Test St, Nashville, TN"}}, "meta":{"ok":True}}
}
raw = json.dumps(merged, ensure_ascii=False)

# upsert-ish: delete any prior row for this lead_key to keep DB clean
cur.execute("DELETE FROM attom_enrichments WHERE lead_key=?", (lead_key,))

cur.execute("""
INSERT INTO attom_enrichments (lead_key, status, attom_raw_json, avm_value, avm_low, avm_high, confidence, enriched_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
""", (lead_key, "synthetic", raw, 123456.0, 111111.0, 135000.0, None, datetime.datetime.utcnow().isoformat()+"Z"))
con.commit()

row = cur.execute("SELECT id, attom_raw_json FROM attom_enrichments WHERE lead_key=? ORDER BY id DESC LIMIT 1", (lead_key,)).fetchone()
_id, raw2 = row
d = json.loads(raw2)

print("INSERTED_ID", _id)
print("PARSED_KEYS", list(d.keys()))
print("AVM_KEYS", list(d["avm"].keys())[:10])
print("DETAIL_KEYS", list(d["detail"].keys())[:10])

con.close()
