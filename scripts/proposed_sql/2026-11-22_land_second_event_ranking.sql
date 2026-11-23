-- PROPOSED, NOT APPLIED. Coordinator-owned change to lead_quality_reasons().
-- Written by the land lane (bots branch feature/pilot-land) for review.
--
-- Why: a land_lane row (HEIRS_PROPERTY / ABSENTEE_LAND) is staged on an
-- ownership signal alone: acreage, an ET AL or heirs name, an out-of-state
-- mailing address. That is a prospect, not a reason to sell now. On the
-- 2026-11-22 snapshot 1,263 of the 1,332 land_lane rows hold a Tennessee rank
-- (858 HEIRS_PROPERTY, 405 ABSENTEE_LAND) and none has a current second event
-- on its parcel. The scheduled land_review bot now writes
-- raw_payload.pilot_review (lane "land") on every tract that has a candidate
-- event; route first_trace_review means a current tax, foreclosure, partition
-- or estate event is tied to the exact parcel, the owner group is identified
-- and the county value is sourced.
--
-- Rule: a land_lane row ranks only while its land card says first_trace_review.
-- Rows from other sources (tax, foreclosure, probate) keep their own rules,
-- including rows for the same parcel.
--
-- Expected on the 2026-11-22 snapshot after refresh_lead_quality():
--   ranked land_lane rows 1,263 -> 0 (no land card is first_trace_review today)
--   every other bot_source unchanged, relative order kept
--   the exact 1,263 staging IDs are in
--   falco-workspace/reports/2026-11-22-pilot-land-receipt.json
--   (ranking_request.expected_to_leave_ranked_ids)
-- Three land tracts hold an estate research card (256a442b, 2d027fcb, 881ebe60):
-- they leave the ranked list with the rest and stay reachable as research.
--
-- "Corroborated land view" (site, coordinator-owned): today it reads
-- property_lane.land_qualification.status = 'corroborated_opportunity', which
-- counts only printed-parcel tax notices and has never been computed in
-- production. Suggested definition: pilot_review.lane = 'land' and
-- pilot_review.route = 'first_trace_review'; research view: lane = 'land' and
-- route = 'research_hold'.

do $migration$
declare
  definition text := pg_get_functiondef('public.lead_quality_reasons(public.homeowner_requests_staging)'::regprocedure);
  anchor text := '  SELECT array_remove(ARRAY[';
  rule text := 'CASE WHEN s.bot_source = ''land_lane'' AND NOT (coalesce(s.raw_payload->''pilot_review''->>''lane'', '''') = ''land'' '
            || 'AND coalesce(s.raw_payload->''pilot_review''->>''route'', '''') = ''first_trace_review'') '
            || 'THEN ''land_no_current_second_event'' END,';
begin
  if strpos(definition, rule) = 0 then
    if array_length(string_to_array(definition, anchor), 1) <> 2 then
      raise exception 'Unexpected lead_quality_reasons definition';
    end if;
    execute replace(definition, anchor, anchor || chr(10) || '    ' || rule);
  end if;
end $migration$;

-- Checks to run inside a transaction that is rolled back before any apply:
--   select count(*) from homeowner_requests_staging s
--    where s.bot_source = 'land_lane' and not ('land_no_current_second_event' = any(lead_quality_reasons(s)));
--     -- expect 0 today
--   select count(*) from homeowner_requests_staging s
--    where s.bot_source <> 'land_lane' and 'land_no_current_second_event' = any(lead_quality_reasons(s));
--     -- expect 0 always
