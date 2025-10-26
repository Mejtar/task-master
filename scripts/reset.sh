#!/bin/bash

# ==================== reset del directorio =======================

# Colores
readonly readonly RED='\033[0;31m'
readonly GREEN='\033[0;32m'
readonly YELLOW='\033[1;33m'
readonly NC='\033[0m'


# Configuracion de rutas
readonly MAINPATH="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly PROCESSEDPATH="${MAINPATH}/../processed"
readonly INPUTPATH="${MAINPATH}/../input"

# Temporal, elimina los logs mientras esta en desarrollo
readonly LOGPATH="${MAINPATH}/../logs"
readonly DELFILE="${LOGPATJ}/*.log"


# Controls de logs

del_logs() {
	shopt -s nullglob
	local files=("${LOGPATH}"/*"${LOGFILE}")
	
	if [[ ${#files[@]} -eq 0  ]]; then
		echo -e  "${YELLOW}[ERROR] No se encuentran logs con los patrones establecidos"
	fi

	for f in "${files[@]}"; do
		rm -f "$f" && echo "Deleted $f"
	done
}


# Control de directorios

init() {
	local dirs=(
		"${PROCESSEDPATH}/transferencias"
		"${PROCESSEDPATH}/pagos"
		"${PROCESSEDPATH}/debines"
		"${PROCESSEDPATH}/failed"
	)


	for dir in "${dirs[@]}"; do
		if [ -n "$(ls -A "$dir" 2>/dev/null)" ]; then
			echo -e "${GREEN}[FIND] Archivos ubicados en directorios"
			echo -e "${YELLOW}[MOVING] Moviendo Archivos..."
			mv "$dir"/* "$INPUTPATH"/ 2>/dev/null
		else
			echo -e "${RED}[FIND] No hay archivos en los directorios"
			exit 1
		fi
	done
	
}
del_logs #Ejecuto la prueba de los logs



init
