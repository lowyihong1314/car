#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="$ROOT_DIR/venv"
PYTHON_CMD="python3"
INSTALL_SYSTEM_PACKAGES=1
INSTALL_PLAYWRIGHT=1
PLAYWRIGHT_WITH_DEPS=0
BOOTSTRAP_DB=1
REBUILD_DB=0

RAW_XLSX="$ROOT_DIR/Carlist_Type_Mapping_RAW.xlsx"
PYTHON_DB="$ROOT_DIR/result_using_python/carlist_type_mapping_python.db"
PYTHON_REQ="$ROOT_DIR/result_using_python/requirements.txt"
AI_REQ="$ROOT_DIR/result_using_AI/requirements.txt"
AI_ENV_EXAMPLE="$ROOT_DIR/result_using_AI/.env.example"
AI_ENV="$ROOT_DIR/result_using_AI/.env"

log() {
  printf '[setup] %s\n' "$*"
}

warn() {
  printf '[setup] warning: %s\n' "$*" >&2
}

die() {
  printf '[setup] error: %s\n' "$*" >&2
  exit 1
}

usage() {
  cat <<'EOF'
Usage: ./setup_linux.sh [options]

Options:
  --python CMD             Python command to use. Default: python3
  --skip-system-packages   Do not try to install Linux packages with apt
  --skip-playwright        Skip Playwright browser installation
  --with-playwright-deps   Install Playwright Chromium with Linux system deps
  --skip-db                Skip SQLite bootstrap/import
  --rebuild-db             Rebuild SQLite DB from Carlist_Type_Mapping_RAW.xlsx
  -h, --help               Show this help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --python)
      [[ $# -ge 2 ]] || die "--python requires a value"
      PYTHON_CMD="$2"
      shift 2
      ;;
    --skip-system-packages)
      INSTALL_SYSTEM_PACKAGES=0
      shift
      ;;
    --skip-playwright)
      INSTALL_PLAYWRIGHT=0
      shift
      ;;
    --with-playwright-deps)
      PLAYWRIGHT_WITH_DEPS=1
      shift
      ;;
    --skip-db)
      BOOTSTRAP_DB=0
      shift
      ;;
    --rebuild-db)
      REBUILD_DB=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      die "Unknown option: $1"
      ;;
  esac
done

[[ "$(uname -s)" == "Linux" ]] || die "This setup script is for Linux only."
cd "$ROOT_DIR"

install_base_packages() {
  if [[ "$INSTALL_SYSTEM_PACKAGES" -eq 0 ]]; then
    log "Skipping Linux package install."
    return
  fi

  if command -v "$PYTHON_CMD" >/dev/null 2>&1 && "$PYTHON_CMD" -m venv --help >/dev/null 2>&1; then
    log "System Python and venv support already look available."
    return
  fi

  if ! command -v apt-get >/dev/null 2>&1; then
    die "Python with venv support is missing. Install python3, python3-venv, and python3-pip, then rerun."
  fi

  local -a prefix=()
  if [[ "$EUID" -ne 0 ]]; then
    command -v sudo >/dev/null 2>&1 || die "Need sudo to install Linux packages with apt."
    prefix=(sudo)
  fi

  log "Installing base Linux packages with apt..."
  "${prefix[@]}" apt-get update
  "${prefix[@]}" apt-get install -y python3 python3-venv python3-pip git ca-certificates curl
}

ensure_python() {
  command -v "$PYTHON_CMD" >/dev/null 2>&1 || die "Python command not found: $PYTHON_CMD"
  "$PYTHON_CMD" -m venv --help >/dev/null 2>&1 || die "Python venv support is missing for: $PYTHON_CMD"
}

create_venv() {
  if [[ ! -x "$VENV_DIR/bin/python" ]]; then
    log "Creating virtual environment at $VENV_DIR"
    "$PYTHON_CMD" -m venv "$VENV_DIR"
  else
    log "Reusing existing virtual environment at $VENV_DIR"
  fi
}

install_python_requirements() {
  local vpy="$VENV_DIR/bin/python"
  log "Upgrading pip tooling..."
  "$vpy" -m pip install --upgrade pip setuptools wheel

  log "Installing Python requirements for result_using_python..."
  "$vpy" -m pip install -r "$PYTHON_REQ"

  log "Installing Python requirements for result_using_AI..."
  "$vpy" -m pip install -r "$AI_REQ"
}

install_playwright() {
  local vpy="$VENV_DIR/bin/python"

  if [[ "$INSTALL_PLAYWRIGHT" -eq 0 ]]; then
    log "Skipping Playwright browser install."
    return
  fi

  if [[ "$PLAYWRIGHT_WITH_DEPS" -eq 1 ]]; then
    log "Installing Playwright Chromium with Linux dependencies..."
    "$vpy" -m playwright install --with-deps chromium
    return
  fi

  if command -v apt-get >/dev/null 2>&1; then
    log "Trying Playwright Chromium install with Linux dependencies..."
    if "$vpy" -m playwright install --with-deps chromium; then
      return
    fi
    warn "Playwright dependency install failed. Falling back to browser-only install."
  fi

  log "Installing Playwright Chromium..."
  if ! "$vpy" -m playwright install chromium; then
    warn "Playwright browser install did not fully complete."
    warn "If browser mode fails later, rerun:"
    warn "  ./setup_linux.sh --with-playwright-deps"
  fi
}

bootstrap_database() {
  local vpy="$VENV_DIR/bin/python"

  if [[ "$BOOTSTRAP_DB" -eq 0 ]]; then
    log "Skipping SQLite bootstrap."
    return
  fi

  if [[ "$REBUILD_DB" -eq 0 && -f "$PYTHON_DB" ]]; then
    log "SQLite DB already exists at $PYTHON_DB"
    return
  fi

  [[ -f "$RAW_XLSX" ]] || die "Missing source workbook: $RAW_XLSX"

  log "Importing $RAW_XLSX into SQLite..."
  (
    cd "$ROOT_DIR/result_using_python"
    "$vpy" import_xlsx_to_sqlite.py --xlsx ../Carlist_Type_Mapping_RAW.xlsx --db carlist_type_mapping_python.db
  )
}

prepare_ai_env() {
  if [[ ! -f "$AI_ENV_EXAMPLE" ]]; then
    warn "Missing AI env template: $AI_ENV_EXAMPLE"
    return
  fi

  if [[ -f "$AI_ENV" ]]; then
    log "Keeping existing AI env file at $AI_ENV"
    return
  fi

  cp "$AI_ENV_EXAMPLE" "$AI_ENV"
  log "Created $AI_ENV from template. Fill in OPENAI_API_KEY before using the AI script."
}

print_next_steps() {
  cat <<EOF

Setup complete.

Python crawler:
  source "$VENV_DIR/bin/activate"
  cd "$ROOT_DIR/result_using_python"
  python classify_with_crawler.py --db carlist_type_mapping_python.db --allow-unknown

Web console:
  source "$VENV_DIR/bin/activate"
  cd "$ROOT_DIR/result_using_python"
  python run_web_console.py --db carlist_type_mapping_python.db --port 3300

AI workflow:
  edit "$AI_ENV"
  source "$VENV_DIR/bin/activate"
  cd "$ROOT_DIR/result_using_AI"
  python run_type_fill.py
EOF
}

install_base_packages
ensure_python
create_venv
install_python_requirements
install_playwright
bootstrap_database
prepare_ai_env
print_next_steps
