import os, sqlite3, json, re
p=os.environ["FALCO_SQLITE_PATH"]
con=sqlite3.connect(p)
cur=con.cursor()

candidates = [
 ('db809279ca102ef3e87be7d098c45f42da09d96c','1409 Scarcroft Lane, Nashville, TN 37221'),
 ('96f145db74a7ae78c285210eacdf642a9ce34699','817 Joseph Avenue, Nashville, TN 37207'),
 ('f6560628f9b5f7bfca7be631fc21656b3086c93e','104 Creighton Ave Nashville, TN 37206'),
 ('beb0bfd62d8d970c3e82e38c55c57c48bce95e2b','323 WITHAM COURT, GOODLETTSVILLE, TN 37072'),
 ('7543a33d7b175ccaeb2cd6fc74716e3052c31150','104 Solway Court, Nashville, TN 37209'),
]

cols=[r[1] for r in cur.execute("PRAGMA table_info(attom_enrichments)").fetchall()]
print("ATTOM_COLS", cols)

def parse_equity(attom_raw_json, avm_value):
    # Conservative: equity only if we can see a mortgage/loan-ish number AND it is well below AVM.
    # We will try a few common keys; otherwise return None.
    try:
        data=json.loads(attom_raw_json) if isinstance(attom_raw_json,str) else (attom_raw_json or {})
    except Exception:
        data={}
    # search numbers that look like loan balances / mortgages in common structures
    keys = [
        ("mortgage", "amount"),
        ("mortgage", "mortgageAmount"),
        ("mortgage", "origLoanAmount"),
        ("sale", "amount"),
        ("assessment", "totalValue"),
        ("avm", "amount"),
    ]
    found=[]
    # recursive walk to find fields named like balance/amount/loan
    def walk(o, path=""):
        if isinstance(o, dict):
            for k,v in o.items():
                np = f"{path}.{k}" if path else k
                if isinstance(v,(dict,list)):
                    walk(v,np)
                else:
                    lk=str(k).lower()
                    if any(s in lk for s in ["loan","mort","balance","amt","amount","debt"]):
                        if isinstance(v,(int,float)) and v>0:
                            found.append((np,float(v)))
                        elif isinstance(v,str):
                            m=re.findall(r"\d{2,}", v.replace(",",""))
                            if m:
                                try: found.append((np,float(m[0])))
                                except: pass
        elif isinstance(o, list):
            for i,v in enumerate(o):
                walk(v, f"{path}[{i}]")
    walk(data)

    # pick the smallest plausible "debt" among found values above a floor to avoid tiny noise
    debt_candidates=[v for _,v in found if v>=10000]
    debt = min(debt_candidates) if debt_candidates else None

    if not avm_value or not debt:
        return None

    equity = avm_value - debt
    ltv = debt / avm_value if avm_value else None
    return {"debt_est": debt, "equity_est": equity, "ltv_est": ltv}

print("EQUITY CHECK (conservative, derived from attom_raw_json)")
for lead_key, addr in candidates:
    row=cur.execute("SELECT status, avm_value, avm_low, avm_high, confidence, attom_raw_json FROM attom_enrichments WHERE lead_key=? ORDER BY id DESC LIMIT 1", (lead_key,)).fetchone()
    if not row:
        print(" -", lead_key, "NO_ATTOM_ROW")
        continue
    status, avm, low, high, conf, raw = row
    eq = parse_equity(raw, avm)
    print(" -", lead_key, "|", addr)
    print("   ", {"status":status,"avm":avm,"low":low,"high":high,"conf":conf,"equity_calc":eq})

con.close()
