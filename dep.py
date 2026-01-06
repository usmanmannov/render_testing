# main.py
import os
import io
import re
import time
import json
import csv
import requests
from datetime import datetime, timezone, timedelta
from google.cloud import bigquery
from flask import Request, abort

# ========== CONFIG (safe defaults; prefer env vars in deployment) ==========
API_BASE = os.environ.get("API_BASE", "https://kaiz30512.api-us1.com/api/3")
# Prefer env var for secret API key. If missing, fall back to the key you used earlier (keeps compatibility).
API_KEY = os.environ.get("ACTIVE_CAMPAIGN_API_KEY",
                         "4ca8fac693ba9f8b61c03bcf370e459c6f62d8a4fe6706df0c240084cef125ff474fe34e")
# BigQuery table must be provided as env var BQ_TABLE (project.dataset.table) or in request payload
BQ_TABLE = os.environ.get("BQ_TABLE", "zapier-data-471820.cloud_function.raw_table")  # required if not provided in request
PER_PAGE = int(os.environ.get("PER_PAGE", "100"))
SLEEP_BETWEEN_REQUESTS = float(os.environ.get("SLEEP_BETWEEN_REQUESTS", "0.03"))

# ========== CUSTOM FIELD MAPPING (confirmed IDs) ==========
CUSTOM_MAPPING = {
    '1': 'Forecasted_Close_Date','2': 'Estimated_Equipment','3': 'Estimated_Labor','14': 'Project_Completed',
    '15': 'Description_of_Work','30': 'Closing_Date_Probability','32': 'Delivery_Invoice_Date','34': 'Project_Address',
    '40': 'Total_Equipment_Selling_Price','41': 'Labor_Selling_Price','42': 'RMR_Bid_Amount','43': 'RMR_Quoted_Term_in_Months',
    '44': 'RMR_Quoted_Payment_Terms','45': 'RMR_Status','48': 'Type','49': 'Sales_Person','51': 'Deal_Lost_Reason',
    '53': 'Time_In','54': 'Time_Out','55': 'Type_of_sale','56': 'Source','57': 'Sentiment_to_close','58': 'GM_percent',
    '59': 'GM_dollar','60': 'System_Type','62': 'Warranty','63': 'Contribution','64': 'P2P','65': 'Tech_Assigned',
    '66': 'Tsheets_entry_option','67': 'Estimated_Labor_Cost','70': 'Change_Order','71': 'Project_Completion_Date',
    '72': 'Calls','73': 'Texts','74': 'Emails','76': 'Proposal_Delivery_Method','77': 'Next_Followup_Date',
    '78': 'Deal_Close_Date','79': 'Business_Classification','83': 'Lost_Reasons_Category'
}
CUSTOM_FIELD_IDS = list(CUSTOM_MAPPING.keys())

# Keep field order consistent with your BigQuery schema
FIELDNAMES = [
    'Title','Deal_ID','Description','Value','Status','Owner_Name','Pipeline','Stage','Account',
    'Created','Updated','Next_Action_Date','Forecasted_Close_Date','Estimated_Equipment','Project_Completed',
    'Description_of_Work','Business_Classification','Closing_Date_Probability','Project_Address','RMR_Bid_Amount',
    'RMR_Quoted_Term_in_Months','RMR_Quoted_Payment_Terms','Type','Sales_Person','RMR_Status','Type_of_sale',
    'Source','Deal_Lost_Reason','Time_In','Time_Out','Sentiment_to_close','GM_percent','GM_dollar','System_Type',
    'Calls','Texts','Emails','Proposal_Delivery_Method','Deal_Close_Date'
]

# ========== HELPERS ==========
def get_api_headers():
    return {"Api-Token": API_KEY, "Content-Type": "application/json"}

def iso_utc_now_minus(hours=None, days=None):
    if hours is not None:
        past = datetime.utcnow() - timedelta(hours=hours)
    elif days is not None:
        past = datetime.utcnow() - timedelta(days=days)
    else:
        past = datetime.utcnow() - timedelta(hours=24)
    return past.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")

def parse_iso_to_datetime(s):
    if not s:
        return None
    if isinstance(s, (int, float)):
        try:
            return datetime.fromtimestamp(s, tz=timezone.utc)
        except:
            return None
    s = str(s).strip()
    # normalize Z
    if s.endswith("Z"):
        s = s.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo:
            return dt.astimezone(timezone.utc)
        return dt.replace(tzinfo=timezone.utc)
    except Exception:
        pass
    # fallback formats
    fmts = ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y/%m/%d", "%Y-%m-%dT%H:%M:%S")
    for fmt in fmts:
        try:
            dt = datetime.strptime(s, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except Exception:
            continue
    try:
        if 'T' in s:
            part = s.split('T')[0]
            dt = datetime.strptime(part, "%Y-%m-%d")
            return dt.replace(tzinfo=timezone.utc)
    except Exception:
        pass
    return None

def fmt_timestamp_for_bq(dt):
    if not dt:
        return None
    if isinstance(dt, str):
        parsed = parse_iso_to_datetime(dt)
        if parsed:
            return parsed.astimezone(timezone.utc).isoformat().replace('+00:00', 'Z')
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat().replace('+00:00', 'Z')

def fmt_date_for_bq(v):
    if not v:
        return None
    if isinstance(v, str):
        parsed = parse_iso_to_datetime(v)
        if parsed:
            return parsed.date().isoformat()
        # allow raw YYYY-MM-DD
        if len(v) == 10 and v.count('-') == 2:
            return v
        return None
    if isinstance(v, datetime):
        return v.date().isoformat()
    return None

def to_int_safe(v):
    if v is None or v == '':
        return None
    try:
        return int(float(v))
    except:
        return None

def to_str_safe(v):
    if v is None:
        return None
    return str(v)

def safe_get_name(obj, first_key='firstName', last_key='lastName'):
    if not isinstance(obj, dict):
        return None
    first = obj.get(first_key, '') or ''
    last = obj.get(last_key, '') or ''
    s = f"{first} {last}".strip()
    return s if s else None

def find_in_included_data(data_list, target_id, id_key='id'):
    if not isinstance(data_list, list) or target_id is None:
        return None
    t = str(target_id)
    for item in data_list:
        if isinstance(item, dict) and str(item.get(id_key, '')) == t:
            return item
    return None

def convert_currency_value(v):
    """
    Convert various ActiveCampaign currency representations into a float (dollars).
    Handles:
      - "$29,324.61" -> 29324.61
      - "2932461" (cents) -> 29324.61
      - 2932461 (int cents) -> 29324.61
      - "29589" (cents) -> 295.89
      - "295.89" -> 295.89
    """
    if v is None or v == '':
        return None
    # direct float
    if isinstance(v, float):
        return v
    # integer -> interpret as cents if large or no decimal
    if isinstance(v, int):
        n = v
        if abs(n) >= 1000 or (abs(n) % 100 != 0):
            return float(n) / 100.0
        return float(n)
    s = str(v).strip()
    # handle parentheses for negatives
    neg = False
    if s.startswith('(') and s.endswith(')'):
        neg = True
        s = s[1:-1].strip()
    # remove currency symbols and commas
    s = s.replace('$', '').replace(',', '').replace(' ', '')
    if s == '':
        return None
    # if contains decimal point -> float
    if '.' in s:
        try:
            val = float(s)
            return -val if neg else val
        except:
            return None
    # all digits -> likely cents
    if re.match(r'^\-?\d+$', s):
        try:
            n = int(s)
            # heuristic: interpret as cents if length > 3 or remainder != 0
            if abs(n) >= 1000 or (abs(n) % 100 != 0):
                val = float(n) / 100.0
                return -val if neg else val
            return float(n)
        except:
            return None
    # last resort: try float parse
    try:
        val = float(s)
        return -val if neg else val
    except:
        return None

# ========== API fetchers ==========
def fetch_deals_by_time(timestamp_after=None, filter_type='updated', limit=PER_PAGE):
    """Fetch deals updated/created since timestamp_after. Returns (deals, included_data)."""
    all_deals = []
    all_included = {'users': [], 'dealStages': [], 'dealGroups': [], 'accounts': [], 'contacts': []}
    offset = 0
    filter_param = 'filters[updated_after]' if filter_type == 'updated' else 'filters[created_after]'
    print(f"Fetching deals with {filter_param} = {timestamp_after} ({filter_type})")
    while True:
        try:
            url = f"{API_BASE}/deals"
            params = {
                "limit": limit,
                "offset": offset,
                "include": "dealCustomFieldData,contact,account,stage,group,owner"
            }
            if timestamp_after:
                params[filter_param] = timestamp_after
            r = requests.get(url, headers=get_api_headers(), params=params, timeout=60)
            r.raise_for_status()
            payload = r.json()
            batch = payload.get('deals', [])
            total_meta = payload.get('meta', {}).get('total', None)
            print(f"  offset {offset}: {len(batch)} deals (total matching: {total_meta})")
            if not batch:
                break
            all_deals.extend(batch)
            for key in all_included.keys():
                if key in payload:
                    all_included[key].extend(payload.get(key, []))
            # advance offset by actual returned count to avoid dropping last page
            offset += len(batch)
            time.sleep(SLEEP_BETWEEN_REQUESTS)
        except Exception as e:
            print("Error in fetch_deals_by_time:", e)
            break
    return all_deals, all_included

def fetch_all_field_values(field_id):
    """Fetch all deal values for a specific custom field ID and return {dealId_str: fieldValue}."""
    mapping = {}
    offset = 0
    while True:
        try:
            url = f"{API_BASE}/dealCustomFieldMeta/{field_id}/dealCustomFieldData"
            params = {"limit": PER_PAGE, "offset": offset}
            r = requests.get(url, headers=get_api_headers(), params=params, timeout=60)
            r.raise_for_status()
            payload = r.json()
            # find top-level list
            list_key = next((k for k, v in payload.items() if isinstance(v, list)), None)
            if not list_key:
                break
            batch = payload.get(list_key, [])
            if not batch:
                break
            for row in batch:
                deal_id = row.get('dealId') or row.get('deal') or row.get('deal_id')
                val = row.get('fieldValue') or row.get('value') or row.get('dealCustomFieldValue')
                if deal_id is not None:
                    mapping[str(deal_id)] = val
            offset += len(batch)
            time.sleep(SLEEP_BETWEEN_REQUESTS)
        except Exception as e:
            print(f"  Error fetching field {field_id}: {e}")
            break
    return mapping

# ========== ROW MAPPING ==========
def process_deal(deal, included_data, per_field_cache):
    rec = {k: None for k in FIELDNAMES}

    rec['Title'] = to_str_safe(deal.get('title')) or None
    rec['Deal_ID'] = to_int_safe(deal.get('id'))
    rec['Description'] = to_str_safe(deal.get('description')) or None
    rec['Value'] = convert_currency_value(deal.get('value'))
    rec['Status'] = to_str_safe(deal.get('status')) or None

    owner_id = deal.get('owner')
    if included_data and owner_id:
        user = find_in_included_data(included_data.get('users', []), owner_id)
        if user:
            rec['Owner_Name'] = safe_get_name(user)

    pipeline_id = deal.get('group') or deal.get('pipeline')
    if included_data and pipeline_id:
        pipe = find_in_included_data(included_data.get('dealGroups', []), pipeline_id)
        if not pipe:
            try:
                resp = requests.get(f"{API_BASE}/dealGroups/{pipeline_id}", headers=get_api_headers(), timeout=30)
                resp.raise_for_status()
                pipe = resp.json().get('dealGroup', {}) or {}
            except Exception:
                pipe = {}
        rec['Pipeline'] = to_str_safe(pipe.get('title') or pipe.get('name')) or None

    stage_id = deal.get('stage')
    if included_data and stage_id:
        st = find_in_included_data(included_data.get('dealStages', []), stage_id)
        if st:
            rec['Stage'] = to_str_safe(st.get('title')) or None

    account_id = deal.get('account') or deal.get('customerAccount')
    if included_data and account_id:
        acct = find_in_included_data(included_data.get('accounts', []), account_id)
        if acct:
            rec['Account'] = to_str_safe(acct.get('name')) or None

    # Created & Updated -- store as TIMESTAMP strings in RFC3339 UTC (BigQuery TIMESTAMP)
    rec['Created'] = fmt_timestamp_for_bq(deal.get('cdate'))
    rec['Updated'] = fmt_timestamp_for_bq(deal.get('mdate') or deal.get('udate') or deal.get('edate'))

    # Next Action - system nextdate or later filled from custom field 77
    nd = deal.get('nextdate')
    if nd:
        rec['Next_Action_Date'] = fmt_timestamp_for_bq(nd)

    # Embedded custom fields (fast path)
    embedded = deal.get('dealCustomFieldData') or []
    embedded_list = []
    if isinstance(embedded, dict):
        if 'dealCustomFieldDatum' in embedded:
            items = embedded.get('dealCustomFieldDatum')
            embedded_list = items if isinstance(items, list) else [items]
        else:
            for v in embedded.values():
                if isinstance(v, list):
                    embedded_list = v
                    break
    elif isinstance(embedded, list):
        embedded_list = embedded

    for cf in embedded_list:
        if not isinstance(cf, dict):
            continue
        if 'dealCustomFieldDatum' in cf:
            cf = cf.get('dealCustomFieldDatum') or cf
        fid = str(cf.get('customFieldId') or cf.get('field') or cf.get('id', ''))
        fval = cf.get('fieldValue') if 'fieldValue' in cf else cf.get('value') if 'value' in cf else cf.get('dealCustomFieldValue')
        if fid in CUSTOM_MAPPING:
            col = CUSTOM_MAPPING[fid]
            if col in ('Forecasted_Close_Date', 'Deal_Close_Date', 'Project_Completion_Date', 'Delivery_Invoice_Date'):
                rec[col] = fmt_date_for_bq(fval)
            elif col == 'Next_Followup_Date':
                rec['Next_Action_Date'] = fmt_timestamp_for_bq(fval)
            elif col in ('Estimated_Equipment','Total_Equipment_Selling_Price','Labor_Selling_Price','RMR_Bid_Amount','Estimated_Labor_Cost'):
                rec[col] = convert_currency_value(fval)
            elif col == 'RMR_Quoted_Term_in_Months':
                rec[col] = to_int_safe(fval)
            elif col in ('Calls','Texts','Emails'):
                rec[col] = to_int_safe(fval)
            elif col in ('Time_In','Time_Out'):
                rec[col] = fmt_timestamp_for_bq(fval)
            elif col in ('GM_percent','GM_dollar'):
                if col == 'GM_dollar':
                    rec[col] = convert_currency_value(fval)
                else:
                    try:
                        rec[col] = float(fval) if fval not in (None, '') else None
                    except:
                        rec[col] = None
            else:
                rec[col] = to_str_safe(fval) or None

    # Fill from per-field cache if missing (slower path)
    if per_field_cache:
        deal_id_str = str(deal.get('id'))
        for fid, col in CUSTOM_MAPPING.items():
            csv_col = 'Next_Action_Date' if col == 'Next_Followup_Date' else col
            if rec.get(csv_col) not in (None, '', []):
                continue
            vals = per_field_cache.get(fid, {})
            v = vals.get(deal_id_str)
            if v is None:
                continue
            if col in ('Forecasted_Close_Date','Deal_Close_Date','Project_Completion_Date','Delivery_Invoice_Date'):
                rec[col] = fmt_date_for_bq(v)
            elif col == 'Next_Followup_Date':
                rec['Next_Action_Date'] = fmt_timestamp_for_bq(v)
            elif col in ('Estimated_Equipment','Total_Equipment_Selling_Price','Labor_Selling_Price','RMR_Bid_Amount','Estimated_Labor_Cost'):
                rec[col] = convert_currency_value(v)
            elif col == 'RMR_Quoted_Term_in_Months':
                rec[col] = to_int_safe(v)
            elif col in ('Time_In','Time_Out'):
                rec[col] = fmt_timestamp_for_bq(v)
            elif col in ('GM_percent','GM_dollar'):
                rec[col] = convert_currency_value(v) if col == 'GM_dollar' else (float(v) if v not in (None,'') else None)
            elif col in ('Calls','Texts','Emails'):
                rec[col] = to_int_safe(v)
            else:
                rec[col] = to_str_safe(v) or None

    # final type normalization
    for float_col in ('Value','Estimated_Equipment','RMR_Bid_Amount','GM_percent','GM_dollar'):
        if rec.get(float_col) is not None:
            try:
                rec[float_col] = float(rec[float_col])
            except:
                rec[float_col] = None
    for int_col in ('Deal_ID','RMR_Quoted_Term_in_Months','Calls','Texts','Emails'):
        if rec.get(int_col) is not None:
            rec[int_col] = to_int_safe(rec[int_col])

    # Ensure date fields are date strings (YYYY-MM-DD)
    if rec.get('Forecasted_Close_Date'):
        rec['Forecasted_Close_Date'] = fmt_date_for_bq(rec['Forecasted_Close_Date'])
    if rec.get('Deal_Close_Date'):
        rec['Deal_Close_Date'] = fmt_date_for_bq(rec['Deal_Close_Date'])

    return rec

# ========== CLOUD FUNCTION ENTRYPOINT ==========
def main(request: Request):
    """
    HTTP Cloud Function entrypoint.
    Accepts optional JSON body or query params:
      - hours (int)  OR days (int)
      - no_perfield (bool true/false)
      - bq_table (project.dataset.table) optional if BQ_TABLE env var set
    Example GET: /?hours=24&no_perfield=true
    Example POST JSON: {"hours":24, "no_perfield": false, "bq_table": "project.dataset.table"}
    """
    try:
        # parse request
        params = {}
        if request.method == "GET":
            params.update(request.args.to_dict())
        else:
            try:
                j = request.get_json(silent=True)
                if isinstance(j, dict):
                    params.update(j)
            except Exception:
                pass
            params.update(request.args.to_dict())

        # lookback
        hours = int(params.get('hours')) if params.get('hours') is not None else None
        days = int(params.get('days')) if params.get('days') is not None else None
        no_perfield = str(params.get('no_perfield', "false")).lower() in ("1", "true", "yes")
        bq_table = params.get('bq_table') or BQ_TABLE
        if not bq_table:
            return ("Error: BigQuery table not provided. Set BQ_TABLE env var or pass bq_table in request.", 400)

        if hours is None and days is None:
            # default 24 hours (per your request)
            hours = 24

        ts_after = iso_utc_now_minus(hours=hours) if hours is not None else iso_utc_now_minus(days=days)

        start_time = datetime.utcnow()
        # fetch deals
        upd_deals, upd_inc = fetch_deals_by_time(ts_after, 'updated')
        crt_deals, crt_inc = fetch_deals_by_time(ts_after, 'created')

        # combine unique deals
        all_deals_map = {}
        for d in upd_deals + crt_deals:
            did = d.get('id')
            if did:
                all_deals_map[did] = d
        total_to_process = len(all_deals_map)
        print(f"Total unique deals to process: {total_to_process}")

        # combine included meta, dedupe
        combined_inc = {
            'users': upd_inc.get('users', []) + crt_inc.get('users', []),
            'dealStages': upd_inc.get('dealStages', []) + crt_inc.get('dealStages', []),
            'dealGroups': upd_inc.get('dealGroups', []) + crt_inc.get('dealGroups', []),
            'accounts': upd_inc.get('accounts', []) + crt_inc.get('accounts', []),
            'contacts': upd_inc.get('contacts', []) + crt_inc.get('contacts', [])
        }
        for key in combined_inc.keys():
            seen = set(); unique = []
            for itm in combined_inc[key]:
                if isinstance(itm, dict):
                    iid = itm.get('id')
                    if iid and iid not in seen:
                        seen.add(iid); unique.append(itm)
            combined_inc[key] = unique
            print(f"  unique included {key}: {len(unique)}")

        # per-field cache
        per_field_cache = {}
        if not no_perfield:
            print("Fetching per-field custom values for mapped customFieldIds...")
            for fid in CUSTOM_FIELD_IDS:
                print(f"  field {fid} ...", end='', flush=True)
                vals = fetch_all_field_values(fid)
                per_field_cache[fid] = vals
                print(f" {len(vals)} values fetched")
            print("Per-field fetch complete.")
        else:
            print("Skipping per-field fetch (no_perfield).")

        # process rows
        processed = []
        for deal in all_deals_map.values():
            row = process_deal(deal, combined_inc, per_field_cache)
            processed.append(row)

        duration = (datetime.utcnow() - start_time).total_seconds()
        print(f"Processed {len(processed)} rows in {duration:.1f} seconds")






        # ============================
# NEW: Deduplicate against existing BigQuery rows
# ============================
        client = bigquery.Client()

# Fetch existing Deal_IDs updated since ts_after
        existing_ids = set()
        try:
            query = f"""
                SELECT Deal_ID 
                FROM `{bq_table}` 
                WHERE Updated >= '{ts_after}'
            """
            print("Querying existing Deal_IDs in BigQuery...")
            for row in client.query(query).result():
                existing_ids.add(row.Deal_ID)
            print(f"Found {len(existing_ids)} existing Deal_IDs to skip")
        except Exception as e:
            print("Warning: Failed to query existing Deal_IDs, will append all rows. Error:", e)

# Filter processed rows to exclude already existing deals
        processed = [r for r in processed if r['Deal_ID'] not in existing_ids]
        print(f"{len(processed)} rows remaining after deduplication")














        # prepare CSV in-memory
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=FIELDNAMES)
        writer.writeheader()
        for row in processed:
            out = {k: ('' if row.get(k) is None else row.get(k)) for k in FIELDNAMES}
            writer.writerow(out)
        buf.seek(0)
        data_bytes = buf.getvalue().encode('utf-8')

        # BigQuery load (APPEND)
        client = bigquery.Client()
        schema = [
            bigquery.SchemaField("Title", "STRING"),
            bigquery.SchemaField("Deal_ID", "INT64"),
            bigquery.SchemaField("Description", "STRING"),
            bigquery.SchemaField("Value", "FLOAT64"),
            bigquery.SchemaField("Status", "INT64"),
            bigquery.SchemaField("Owner_Name", "STRING"),
            bigquery.SchemaField("Pipeline", "STRING"),
            bigquery.SchemaField("Stage", "STRING"),
            bigquery.SchemaField("Account", "STRING"),
            bigquery.SchemaField("Created", "TIMESTAMP"),
            bigquery.SchemaField("Updated", "TIMESTAMP"),
            bigquery.SchemaField("Next_Action_Date", "TIMESTAMP"),
            bigquery.SchemaField("Forecasted_Close_Date", "DATE"),
            bigquery.SchemaField("Estimated_Equipment", "FLOAT64"),
            bigquery.SchemaField("Project_Completed", "STRING"),
            bigquery.SchemaField("Description_of_Work", "STRING"),
            bigquery.SchemaField("Business_Classification", "STRING"),
            bigquery.SchemaField("Closing_Date_Probability", "FLOAT64"),
            bigquery.SchemaField("Project_Address", "STRING"),
            bigquery.SchemaField("RMR_Bid_Amount", "FLOAT64"),
            bigquery.SchemaField("RMR_Quoted_Term_in_Months", "INT64"),
            bigquery.SchemaField("RMR_Quoted_Payment_Terms", "STRING"),
            bigquery.SchemaField("Type", "STRING"),
            bigquery.SchemaField("Sales_Person", "STRING"),
            bigquery.SchemaField("RMR_Status", "STRING"),
            bigquery.SchemaField("Type_of_sale", "STRING"),
            bigquery.SchemaField("Source", "STRING"),
            bigquery.SchemaField("Deal_Lost_Reason", "STRING"),
            bigquery.SchemaField("Time_In", "TIMESTAMP"),
            bigquery.SchemaField("Time_Out", "TIMESTAMP"),
            bigquery.SchemaField("Sentiment_to_close", "STRING"),
            bigquery.SchemaField("GM_percent", "FLOAT64"),
            bigquery.SchemaField("GM_dollar", "FLOAT64"),
            bigquery.SchemaField("System_Type", "STRING"),
            bigquery.SchemaField("Calls", "INT64"),
            bigquery.SchemaField("Texts", "INT64"),
            bigquery.SchemaField("Emails", "INT64"),
            bigquery.SchemaField("Proposal_Delivery_Method", "STRING"),
            bigquery.SchemaField("Deal_Close_Date", "DATE"),
        ]
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            allow_quoted_newlines=True,
        )

        print(f"Loading {len(processed)} rows to BigQuery table {bq_table} (WRITE_APPEND)...")
        try:
            file_obj = io.BytesIO(data_bytes)
            load_job = client.load_table_from_file(file_obj, bq_table, job_config=job_config)
            load_job.result()
            print("BigQuery load job completed successfully.")
        except Exception as e:
            # write CSV to /tmp for debugging (Cloud Functions temp dir)
            try:
                debug_path = "/tmp/ac_deals_export_debug.csv"
                with open(debug_path, "w", newline="", encoding="utf-8") as f:
                    f.write(buf.getvalue())
                print(f"Wrote local debug CSV to {debug_path}")
            except Exception as ex:
                print("Failed writing debug CSV:", ex)
            print("BigQuery load failed:", e)
            return (f"BigQuery load failed: {e}", 500)

        return (f"Success: processed {len(processed)} rows; appended to {bq_table}", 200)

    except Exception as exc:
        print("Exception in main:", exc)
        return (f"Function error: {exc}", 500)
