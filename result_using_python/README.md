# result_using_python

Non-AI implementation for filling vehicle `Type` values from free public sources.

## What it does

- Uses [carlist_type_mapping_python.db](/home/yukang/car/result_using_python/carlist_type_mapping_python.db) as the working dataset
- Bootstraps the SQLite DB from [Carlist_Type_Mapping_RAW.xlsx](/home/yukang/car/Carlist_Type_Mapping_RAW.xlsx) on first run or when asked to rebuild
- Reads and updates `Car Info`, `Type`, `Evidence URL`, and `URL Prove`
- Processes at most 10 rows per run by default
- Tries brand-specific official public websites first
- Falls back to free public Wikimedia endpoints when official sites do not yield a result
- Commits each row update immediately so partial progress is not lost

## Allowed output labels

- `diesel`
- `petrol`
- `electric`
- `electric/petrol`
- `unknown`

## Setup

```bash
cd /home/yukang/car
source venv/bin/activate
pip install -r result_using_python/requirements.txt
```

## Run

```bash
cd /home/yukang/car/result_using_python
python3 classify_with_crawler.py
```

This will auto-create [carlist_type_mapping_python.db](/home/yukang/car/result_using_python/carlist_type_mapping_python.db) from [Carlist_Type_Mapping_RAW.xlsx](/home/yukang/car/Carlist_Type_Mapping_RAW.xlsx) if the DB does not exist yet.

To rebuild the DB from the raw workbook:

```bash
python3 import_xlsx_to_sqlite.py
```

Useful options:

```bash
python3 classify_with_crawler.py --db carlist_type_mapping_python.db
python3 classify_with_crawler.py --reimport-db
python3 classify_with_crawler.py --bootstrap-xlsx ../Carlist_Type_Mapping_RAW.xlsx
python3 classify_with_crawler.py --limit 10
python3 classify_with_crawler.py --delay 2.0
python3 classify_with_crawler.py --overwrite
python3 classify_with_crawler.py --allow-unknown
```

## Web Console

Run the Flask controller with SSE live updates on port `3300`:

```bash
cd /home/yukang/car/result_using_python
python3 run_web_console.py --db carlist_type_mapping_python.db
```

Then open:

```text
http://127.0.0.1:3300
```

The dashboard lets you:

- start a classification batch in the browser
- watch row-by-row progress over SSE
- search and filter the SQLite table live
- inspect recent updates without opening Excel

## Notes

- This script does not use any API token.
- It does not use any paid API.
- It now prefers a small brand whitelist of official public websites, then falls back to Wikimedia.
- Official-site crawling is conservative: public pages only, small request volume, sitemap-based discovery, and `robots.txt` checks.
- By default, it only processes rows where `Type` is empty.
- Rows with `Type=unknown` are skipped unless `--allow-unknown` is used.
- The crawler now works directly against SQLite; Excel is only used as a bootstrap source.
- The script now backs off on HTTP 429 and falls back to `unknown` instead of crashing the whole run.
- `URL Prove` is `True` only when the matched page text supports the chosen `Type`.
- It still uses rule-based keyword matching, so accuracy will remain lower than the AI version.
