\# FALCO — SYSTEM STATE (Persistent Transfer File)



This file exists to allow seamless chat transfer.

It must remain architecture-focused and NOT session-specific.



------------------------------------------------------------------



\## 1. ENVIRONMENT



Repo Path:

Database Path:

Python Version:

ATTOM Status:

NOTION Status:

Other External Services:



------------------------------------------------------------------



\## 2. DATABASE SCHEMA STATUS



Migrations Applied:

Current Schema Version:

Critical Tables:

\- leads

\- ingest\_events

\- attom\_enrichments

\- scoring fields

\- grading fields



------------------------------------------------------------------



\## 3. PIPELINE ARCHITECTURE



STAGE 1 — INGESTION

\- Active Bots:

\- Gating Logic:

\- Dedupe Logic:

\- Run ID Handling:



STAGE 2 — ENRICHMENT

\- Candidate Selection Rules:

\- ATTOM Gating Rules:

\- Re-enrichment Policy:

\- Call Cap Policy:



STAGE 2B — COMPS

\- Dependency Requirements:

\- When It Runs:



STAGE 3 — SCORING

\- Scope (run\_id / global):

\- Inputs Required:

\- Output Fields:



STAGE 3B — GRADING

\- Inputs Required:

\- Output Fields:



STAGE 4 — PACKAGING

\- Requirements:

\- External Dependencies:



------------------------------------------------------------------



\## 4. CURRENT ENGINE BEHAVIOR



What is functioning correctly:

What is intentionally gated:

What is disabled:

Known limitations:



------------------------------------------------------------------



\## 5. STRATEGIC OBJECTIVE



Primary Goal:

Current Phase:

Constraints:

Risk Controls:



------------------------------------------------------------------



\## 6. NEXT EXECUTION STEP



Implement controlled ATTOM refresh policy (TTL-based re-enrichment logic) without increasing unnecessary call volume.



------------------------------------------------------------------



\## 7. RULES



\- Do not rewrite architecture without explicit instruction.

\- Do not expand ATTOM calls without gating logic.

\- Maintain deterministic behavior.

\- Always update this file before closing session.

