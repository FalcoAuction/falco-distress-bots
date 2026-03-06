-- Delete duplicates, keeping the row with the highest id per (run_id, lead_key)
DELETE FROM packets
WHERE id NOT IN (
    SELECT MAX(id)
    FROM packets
    GROUP BY run_id, lead_key
);

-- Add unique constraint on (run_id, lead_key)
CREATE UNIQUE INDEX IF NOT EXISTS idx_packets_run_lead
    ON packets(run_id, lead_key);
