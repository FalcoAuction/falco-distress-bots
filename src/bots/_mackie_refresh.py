"""Report completeness and guarded Mackie Wolf date revalidation (no address-only joins)."""
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
import hashlib
import io
import json
import re
from urllib.parse import urlparse, quote
from ._base import BotBase, _supabase, guard_phone_metadata
from ._address import normalize_address
from ._regions import TN_COUNTIES

WRITER = 'mackie_wolf_trustee'
FILE_RE = re.compile(r'\b\d{2}-\d{6}-\d{3}\b')
HEAD_RE = re.compile(r'^\d{1,2}/\d{1,2}/\d{4}\s+(\d{2}-\d{6}-\d{3})\s+', re.M)


def report_as_of(url):
    parsed = urlparse(url)
    if parsed.hostname not in ('mwzmlaw.com', 'www.mwzmlaw.com'):
        return None
    match = re.search(r'TN-Sale-Report-as-(?:of-)?(\d{2}\.\d{2}\.\d{4})\.pdf$', parsed.path, re.I)
    try:
        return datetime.strptime(match[1], '%m.%d.%Y').date() if match else None
    except ValueError:
        return None


def inspect_report(content, url, linked, extractor, today=None):
    today = today or date.today()
    with extractor.open(io.BytesIO(content)) as pdf:
        pages = [p.extract_text() or '' for p in pdf.pages]
    text = '\n'.join(pages)
    as_of = report_as_of(url)
    return {'report_as_of': as_of.isoformat() if as_of else None, 'report_url': url,
            'report_as_of_basis': 'official_link_filename',
            'report_sha256': hashlib.sha256(content).hexdigest(),
            'checked_at': datetime.now(timezone.utc).isoformat(),
            'file_counts': Counter(FILE_RE.findall(text)), 'header_files': HEAD_RE.findall(text),
            'dated_rows': len(re.findall(r'^\s*\d{1,2}/\d{1,2}/\d{4}\b',text,re.M)),
            'pages_readable': bool(pages and all(p.strip() and HEAD_RE.search(p) for p in pages)), 'complete': False,
            'authoritative': bool(linked and as_of and 0 <= (today-as_of).days <= 7)}


def finalize_report(report, leads):
    counts = report['file_counts']
    counties = {c.casefold() for c in TN_COUNTIES}
    safe = []
    for lead in leads:
        file_no = (lead.raw_payload or {}).get('file_no')
        if counts.get(file_no) != 1 or (lead.county or '').casefold() not in counties:
            continue
        try:
            date.fromisoformat(lead.trustee_sale_date)
        except (ValueError, TypeError):
            continue
        safe.append(lead)
    report['complete'] = bool(report['pages_readable'] and counts and all(n == 1 for n in counts.values())
        and report['dated_rows'] == len(report['header_files'])
        and Counter(report['header_files']) == counts
        and Counter((lead.raw_payload or {}).get('file_no') for lead in safe) == counts)
    return safe


def protected(row, report):
    pm = row.get('phone_metadata')
    if pm is not None and not isinstance(pm, dict):
        return True
    pm = pm or {}
    status = pm.get('sale_status')
    legacy = isinstance(status, dict) and status.get('updated_by') == 'trustee_status_reaper' and status.get('status') == 'cancelled'
    if status and not legacy:
        return True
    hold = pm.get('trustee_notice_review')
    if hold and (not isinstance(hold, dict) or hold.get('updated_by') not in (WRITER, 'trustee_status_reaper')):
        return True
    tracking = pm.get('notice_tracking') or {}
    if not isinstance(tracking, dict):
        return True
    if tracking.get('published_sale_date') and tracking['published_sale_date'] != row.get('trustee_sale_date'):
        return True  # Stored date changed independently of the last source observation.
    prior = [tracking.get('report_as_of'), (hold or {}).get('report_as_of')]
    raw = row.get('raw_payload') or {}
    if isinstance(raw, dict):
        prior.append(raw.get('report_date'))
    if any(str(d) > report['report_as_of'] for d in prior if d):
        return True
    try:
        now = datetime.fromisoformat(report['checked_at'])
        return any(datetime.fromisoformat(v.replace('Z','+00:00')) > now
                   for v in (tracking.get('last_seen_at'), row.get('sale_date_last_seen_at')) if v)
    except (ValueError, TypeError, AttributeError):
        return True


def file_identity(row):
    raw = row.get('raw_payload')
    file_no = raw.get('file_no') if isinstance(raw, dict) else None
    if not isinstance(file_no, str) or not FILE_RE.fullmatch(file_no):
        return None
    return file_no if row.get('pipeline_lead_key') == BotBase.make_lead_key('mackie_wolf',file_no) else None


def same_property(row, lead):
    county = lambda c: re.sub(r'\s+county$', '', str(c or '').strip(), flags=re.I).casefold()
    def address(value):
        parsed = normalize_address(value or '')
        return '' if parsed.needs_resolution else (parsed.normalized or '').strip().casefold()
    raw = row.get('raw_payload') or {}
    old_names = raw.get('borrowers') or []
    new_names = (lead.raw_payload or {}).get('borrowers') or []
    if old_names and new_names and {str(n).strip().casefold() for n in old_names} != {str(n).strip().casefold() for n in new_names}:
        return False
    return bool(file_identity(row) == lead.raw_payload['file_no'] and county(row.get('county'))
        and county(row.get('county')) == county(lead.county) and address(row.get('property_address'))
        and re.search(r'\b(?:TN|TENNESSEE)\b', str(row.get('property_address') or ''), re.I)
        and address(row.get('property_address')) == address(lead.property_address))


def refresh_patch(row, report, lead=None, conflict=False):
    if not report.get('authoritative') or protected(row, report):
        return None
    file_no = file_identity(row)
    if not file_no:
        return None
    pm = dict(row.get('phone_metadata') or {})
    evidence = {k: report[k] for k in ('report_as_of','report_as_of_basis','report_url','report_sha256','checked_at')}
    count = report['file_counts'].get(file_no, 0)
    tracking = pm.get('notice_tracking') or {}
    if (tracking.get('report_as_of') == report['report_as_of'] and tracking.get('report_sha256')
            and tracking['report_sha256'] != report['report_sha256']):
        detail = 'report_version_conflict'
    elif conflict:
        detail = 'identity_mismatch'
    elif count > 1:
        detail = 'ambiguous_file'
    elif count and lead is None:
        detail = 'published_unparsed'
    elif not count:
        if not report['complete']:
            return None
        detail = 'missing_from_current_report'
    elif not same_property(row, lead):
        detail = 'identity_mismatch'
    else:
        detail = None
    if detail:
        pm['trustee_notice_review'] = dict(evidence, updated_by=WRITER, reason='stale_notice', detail=detail,
                                           sale_date=row.get('trustee_sale_date'), case_number=file_no)
        return {'phone_metadata': pm} if pm != row.get('phone_metadata') else None
    pm.pop('trustee_notice_review', None)
    legacy = pm.get('sale_status')
    if isinstance(legacy, dict) and legacy.get('updated_by') == 'trustee_status_reaper' and legacy.get('status') == 'cancelled':
        pm.pop('sale_status')
    tracking = dict(pm.get('notice_tracking') or {})
    tracking.update(evidence, case_number=file_no, published_sale_date=lead.trustee_sale_date,
                    last_seen_at=report['checked_at'], last_seen_by=WRITER)
    pm['notice_tracking'] = tracking
    patch = {'phone_metadata': pm, 'trustee_sale_date': lead.trustee_sale_date}
    if 'sale_date_last_seen_at' in row:
        patch['sale_date_last_seen_at'] = report['checked_at']
    return patch


def refresh_staging(bot, leads):
    report = getattr(bot, '_report', None)
    if not report or not report['authoritative']:
        return 0, 0
    client = _supabase()
    if client is None:
        return 0, 0
    base_cols = 'id,pipeline_lead_key,county,property_address,trustee_sale_date,phone_metadata,raw_payload'
    staging = []
    last = None
    while True:
        q = client.table('homeowner_requests_staging').select(base_cols+',staging_status,rejection_reason').eq('bot_source',WRITER).order('id').limit(500)
        if last is not None:
            q = q.gt('id',last)
        page = q.execute().data or []
        staging.extend(page)
        if len(page)<500:
            break
        last = page[-1]['id']
    groups = defaultdict(list)
    for row in staging:
        groups[row['pipeline_lead_key']].append(('homeowner_requests_staging',row))
    keys = sorted(set(groups) | {lead.pipeline_lead_key for lead in leads})
    failed_keys = set()
    for offset in range(0,len(keys),100):
        chunk = keys[offset:offset+100]
        try:
            rows = client.table('homeowner_requests').select(base_cols+',sale_date_last_seen_at').eq('source','bot').in_('pipeline_lead_key',chunk).execute().data or []
            for row in rows:
                groups[row['pipeline_lead_key']].append(('homeowner_requests',row))
        except Exception as exc:
            failed_keys.update(chunk)
            bot.logger.warning('Mackie live identity check failed (%s); batch protected',type(exc).__name__)
    by_key = {lead.pipeline_lead_key:lead for lead in leads}
    fresh = [lead for lead in leads if lead.pipeline_lead_key not in groups and lead.pipeline_lead_key not in failed_keys]
    staged, _ = BotBase._write_staging(bot,fresh) if fresh else (0,0)
    bot.refresh_counts = Counter()
    for key, copies in groups.items():
        if key in failed_keys or any(protected(row,report) for _,row in copies):
            continue
        lead = by_key.get(key)
        conflict = bool(lead and any(not same_property(row,lead) for _,row in copies))
        for table,row in copies:
            patch = refresh_patch(row,report,lead,conflict)
            if patch is None:
                continue
            old_pm = row.get('phone_metadata')
            if len(quote(json.dumps(old_pm),safe=''))>6000:
                bot.refresh_counts['oversized'] += 1
                continue
            q = client.table(table).update(patch).eq('id',row['id']).eq('pipeline_lead_key',key)
            q = q.eq('bot_source',WRITER) if table.endswith('_staging') else q.eq('source','bot')
            guards = ['county','property_address','trustee_sale_date']
            guards += ['staging_status','rejection_reason'] if table.endswith('_staging') else ['sale_date_last_seen_at']
            for field in guards:
                q = q.is_(field,'null') if row.get(field) is None else q.eq(field,row[field])
            try:
                written = guard_phone_metadata(q,old_pm).execute().data
                bot.refresh_counts['updated' if written else 'conflicts'] += 1
            except Exception as exc:
                bot.refresh_counts['failed'] += 1
                bot.logger.warning('Mackie guarded refresh failed (%s)',type(exc).__name__)
    bot.logger.info('Mackie report %s complete=%s refresh=%s',report['report_as_of'],report['complete'],dict(bot.refresh_counts))
    return staged, len(leads)-len(fresh)
