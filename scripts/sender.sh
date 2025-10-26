#!/usr/bin/env bash
set -euo pipefail

#====================== Vars and flag Configurations ==============================
readonly RED='\033[0;31m'
readonly GREEN='\033[0;32m'
readonly YELLOW='\033[1;33m'
readonly NC='\033[0m'

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly LOG_DIR="${SCRIPT_DIR}/../logs"

readonly TO_N="mceronicolas@gmail.com"
readonly TO_Y="yansanjauregui@gmail.com"
readonly NORMALSUBJECT="Aviso de movimiento - $(date '+%Y-%m-%d_%H:%M:%S')"
readonly ERRORSUBJECT="Fallido aviso de movimiento - $(date '+%Y-%m-%d_%H:%M:%S')"

#============================ Loggers ==============================================
readonly LOGFILE="${LOG_DIR}/$(whoami)_$$_$(date '+%Y%m%d_%H%M%S').log"
readonly REGEX_PATTERN="$(whoami)_*_[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]_[0-9][0-9][0-9][0-9][0-9][0-9].log"

find_result=$(find "${LOG_DIR}" -maxdepth 1 -name "${REGEX_PATTERN}" -type f -print -quit)

[[ -n "$find_result" ]] && echo "$find_result" || echo -e "${RED}[ERROR]No se econtro el archivo log${NC}"
#============================ Control ==============================================
validate_filename() {
    local filename="$1"
    [[ -z "$filename" ]] && return 1
    
    if [[ "$filename" =~ ^[a-zA-Z0-9._-]+\.log$ ]]; then
        return 0
    else
        return 1
    fi
}

#============================ Sender ===============================================
if [[ -f "$find_result" ]] && validate_filename "$(basename "$find_result")"; then
    echo "Adjunto log del día $(date)" | mail -s "$NORMALSUBJECT" -A "$find_result" "$TO_N"
else
    echo "Archivo no válido o inexistente: $find_result" >&2
    exit 1
fi