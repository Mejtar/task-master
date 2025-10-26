#!/bin/bash

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly LOGDIRECTORY="${SCRIPT_DIR}/../logs"

shopt -s nullglob
LOGFILES=("$LOGDIRECTORY"/*.log)
shopt -u nullglob

if (( ${#LOGFILES[@]} )); then
    echo -e "Archivos encontrados en $LOGDIRECTORY:" "$1"
    printf ' - %s\n' "${LOGFILES[@]}"
else
    echo "No se encontraron archivos .log en $LOGDIRECTORY"
    echo -e "$1" "$2"
fi


