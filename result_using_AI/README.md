# result_using_AI

Minimal script for filling vehicle `Type` values with the OpenAI API.

## What it does

- Reads [Carlist Type Mapping.xlsx](/home/yukang/car/Carlist%20Type%20Mapping.xlsx)
- Looks for `Car Info` and `Type`
- Adds `Evidence URL` and `URL Prove` next to `Type` if missing
- Splits `Car Info` at the first space and treats the first token as the brand
- First asks the model to classify `Type` in brand-based batches
- Uses up to 10 models per brand batch by default
- Processes up to 3 brand batches per run by default
- Then separately searches for a supporting URL and checks whether that URL really supports the chosen `Type`
- Saves after each row stage so partial progress is not lost
- Skips rows where `Type` is already `diesel`, `petrol`, `electric`, or `electric/petrol`
- By default, Python only sends rows with empty `Type` to AI
- Rows with `Type=unknown` are skipped unless you explicitly enable them
- Writes results to `carlist_type_mapping_ai.xlsx`

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
pip install -r result_using_AI/requirements.txt
```

Create `result_using_AI/.env`:

```bash
OPENAI_API_KEY=your_key_here
OPENAI_MODEL=gpt-5
```

## Run

```bash
cd /home/yukang/car/result_using_AI
python run_type_fill.py
```

Useful options:

```bash
python run_type_fill.py --limit 10
python run_type_fill.py --brand-batches 3
python run_type_fill.py --overwrite
python run_type_fill.py --model gpt-5
python run_type_fill.py --allow-unknown
```

## Notes

- The script only processes rows where `Type` is not already `diesel`, `petrol`, `electric`, or `electric/petrol`.
- By default, Python only processes rows where `Type` is empty.
- `unknown` rows are skipped completely unless `--allow-unknown` is used.
- `--limit` means models per brand batch, not total rows for the run.
- `--brand-batches` controls how many brand batches are sent to AI in one run.
- `--overwrite` still forces re-processing for rows that are selected by that rule.
- The script automatically loads `OPENAI_API_KEY` and `OPENAI_MODEL` from `result_using_AI/.env`.
- It uses live web search only for the evidence step, so the machine running it needs network access.
- `URL Prove` is `True` only when the found page clearly supports the chosen `Type`.
- If the page is ambiguous, unrelated, contradictory, or no supporting URL is found, `URL Prove` is written as `False`.
- The source workbook is not modified unless you point `--output` back to the same file.
