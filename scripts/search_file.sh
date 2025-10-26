#!/usr/bin/env bash
set -euo pipefail

# =================== PRODUCTION FILE DISPATCHER ===================
# Maneja archivos financieros con atomicidad y observabilidad
# Autor: Refactorizado para entorno empresarial
# Versión: 2.0

# =================== CONFIGURACIÓN ===================
readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly INPUT_DIR="${SCRIPT_DIR}/../input"
readonly PROCESSED_BASE="${SCRIPT_DIR}/../processed"
readonly LOG_DIR="${SCRIPT_DIR}/../logs"
readonly LOCKFILE="/tmp/file_dispatcher_$$.lock"
readonly MAX_RETRIES=3
readonly RETRY_DELAY=2

# Colores para terminal (no se escriben en logs)
readonly RED='\033[0;31m'
readonly GREEN='\033[0;32m'
readonly YELLOW='\033[1;33m'
readonly NC='\033[0m'

# Log file con fecha
readonly LOGGER="${LOG_DIR}/transport_$(date '+%Y%m%d').log"

# Contadores globales
declare -gi PROCESSED=0
declare -gi FAILED=0
declare -gi SKIPPED=0

# Buffer de logs
declare -a LOG_BUFFER=()
readonly LOG_BUFFER_SIZE=20

# =================== INICIALIZACIÓN ===================
initialize() {
    # Crear directorios necesarios
    local dirs=(
        "$LOG_DIR"
        "${PROCESSED_BASE}/transferencias"
        "${PROCESSED_BASE}/pagos"
        "${PROCESSED_BASE}/debines"
        "${PROCESSED_BASE}/failed"
    )
    
    for dir in "${dirs[@]}"; do
        if ! mkdir -p "$dir" 2>/dev/null; then
            echo "FATAL: No se puede crear directorio: $dir" >&2
            exit 1
        fi
    done
    
    # Validar directorio de entrada
    if [[ ! -d "$INPUT_DIR" ]]; then
        echo "FATAL: Directorio de entrada no existe: $INPUT_DIR" >&2
        exit 1
    fi
    
    # Validar permisos de escritura
    if [[ ! -w "$LOG_DIR" ]]; then
        echo "FATAL: Sin permisos de escritura en: $LOG_DIR" >&2
        exit 1
    fi
}

# =================== LOGGING ===================
log_raw() {
    local level="$1"
    local msg="$2"
    local timestamp="$(date '+%Y-%m-%d %H:%M:%S')"
    local log_entry="[$timestamp] [$level] $msg"
    
    LOG_BUFFER+=("$log_entry")
    
    # Flush si alcanza tamaño máximo
    if [[ ${#LOG_BUFFER[@]} -ge $LOG_BUFFER_SIZE ]]; then
        flush_logs
    fi
}

log() {
    echo -e "${GREEN}[INFO]${NC} $1"
    log_raw "INFO" "$1"
}

warning() {
    echo -e "${YELLOW}[WARN]${NC} $1" >&2
    log_raw "WARN" "$1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1" >&2
    log_raw "ERROR" "$1"
}

flush_logs() {
    if [[ ${#LOG_BUFFER[@]} -gt 0 ]]; then
        printf '%s\n' "${LOG_BUFFER[@]}" >> "$LOGGER" 2>/dev/null || true
        LOG_BUFFER=()
    fi
}

# =================== FILE LOCKING ===================
acquire_lock() {
    exec 200>"$LOCKFILE"
    
    if ! flock -n 200; then
        error "Otra instancia está ejecutándose (lockfile: $LOCKFILE)"
        exit 1
    fi
    
    log "Lock adquirido: $LOCKFILE"
}

release_lock() {
    if [[ -e "$LOCKFILE" ]]; then
        flock -u 200 2>/dev/null || true
        rm -f "$LOCKFILE"
        log "Lock liberado"
    fi
}

# =================== VALIDACIÓN Y CLASIFICACIÓN ===================
validate_filename() {
    local filename="$1"
    
    # Validar caracteres permitidos y extensión
    if [[ ! "$filename" =~ ^[a-zA-Z0-9_.-]+\.(transf|payf|deb)$ ]]; then
        return 1
    fi
    
    return 0
}

classify_file() {
    local filepath="$1"
    local filename="$(basename "$filepath")"
    
    # Validar nombre
    if ! validate_filename "$filename"; then
        warning "Nombre inválido o sospechoso: $filename"
        return 1
    fi
    
    # Clasificar por extensión
    case "${filename##*.}" in
        transf)
            echo "${PROCESSED_BASE}/transferencias"
            return 0
            ;;
        payf)
            echo "${PROCESSED_BASE}/pagos"
            return 0
            ;;
        deb)
            echo "${PROCESSED_BASE}/debines"
            return 0
            ;;
        *)
            return 1
            ;;
    esac
}

# =================== OPERACIONES ATÓMICAS ===================
atomic_move() {
    local src="$1"
    local dest_dir="$2"
    local filename="$(basename "$src")"
    local dest="${dest_dir}/${filename}"
    local temp_dest="${dest}.tmp.$$"
    
    # Verificar que archivo origen existe (TOCTOU mitigation)
    if [[ ! -f "$src" ]]; then
        warning "Archivo desapareció antes de mover: $filename"
        return 1
    fi
    
    # Verificar que destino no existe (prevenir sobrescritura)
    if [[ -e "$dest" ]]; then
        warning "Archivo ya existe en destino: $filename"
        mv "$src" "${PROCESSED_BASE}/failed/${filename}.duplicate.$(date +%s)" 2>/dev/null || true
        return 1
    fi
    
    # Intentar move directo (más eficiente)
    if mv "$src" "$temp_dest" 2>/dev/null; then
        # Rename atómico
        if mv "$temp_dest" "$dest" 2>/dev/null; then
            return 0
        else
            # Rollback
            mv "$temp_dest" "$src" 2>/dev/null || true
            return 1
        fi
    fi
    
    # Fallback: copy + verify + delete
    if cp -p "$src" "$temp_dest" 2>/dev/null; then
        # Verificar integridad
        if cmp -s "$src" "$temp_dest"; then
            mv "$temp_dest" "$dest" 2>/dev/null || return 1
            rm -f "$src" 2>/dev/null || {
                warning "No se pudo eliminar archivo origen: $filename"
                return 1
            }
            return 0
        else
            rm -f "$temp_dest" 2>/dev/null
            error "Fallo verificación de integridad: $filename"
            return 1
        fi
    fi
    
    return 1
}

# =================== PROCESAMIENTO CON RETRY ===================
process_file_with_retry() {
    local filepath="$1"
    local filename="$(basename "$filepath")"
    local attempt=1
    
    while [[ $attempt -le $MAX_RETRIES ]]; do
        local dest_dir
        
        # Clasificar archivo
        if ! dest_dir="$(classify_file "$filepath")"; then
            warning "No se pudo clasificar: $filename"
            mv "$filepath" "${PROCESSED_BASE}/failed/" 2>/dev/null || true
            ((SKIPPED++))
            return 1
        fi
        
        # Intentar mover
        if atomic_move "$filepath" "$dest_dir"; then
            log "Procesado: $filename → ${dest_dir##*/}"
            ((PROCESSED++))
            return 0
        fi
        
        if [[ $attempt -lt $MAX_RETRIES ]]; then
            warning "Reintento $attempt/$MAX_RETRIES para: $filename"
            sleep $RETRY_DELAY
        fi
        
        ((attempt++))
    done
    
    # Falló todos los intentos
    error "Fallo permanente después de $MAX_RETRIES intentos: $filename"
    mv "$filepath" "${PROCESSED_BASE}/failed/" 2>/dev/null || true
    ((FAILED++))
    return 1
}

# =================== BÚSQUEDA Y PROCESAMIENTO ===================
search_and_process() {
    log "Iniciando búsqueda en: $INPUT_DIR"
    
    local file_count=0
    
    # Buscar archivos con null delimiter (maneja espacios y caracteres especiales)
    while IFS= read -r -d '' filepath; do
        ((file_count++))
        process_file_with_retry "$filepath"
    done < <(find "$INPUT_DIR" -maxdepth 1 -type f \
             \( -name "*.transf" -o -name "*.payf" -o -name "*.deb" \) \
             -print0 2>/dev/null)

    #Verifica si hay archivos con extenciones distintas
    while IFS= read -r -d '' filepath; do
        local filename="$(basename "$filepath")"
        warning "Extensión no reconocida, moviendo a failed/: $filename"
        mv "$filepath" "${PROCESSED_BASE}/failed/${filename}" 2>/dev/null || {
            error "No se pudo mover archivo inválido: $filename"
        }
        ((SKIPPED++))
    done < <(find "$INPUT_DIR" -maxdepth 1 -type f \
			 ! -path "${PROCESSED_BASE}/failed/*" \
			 ! -name "*.invalid.*" \
             ! -name "*.transf" ! -name "*.payf" ! -name "*.deb" \
             -print0 2>/dev/null)
    
    if [[ $file_count -eq 0 ]]; then
        log "No se encontraron archivos para procesar"
        return 0
    fi
    
    # Log de resumen
    log "=== RESUMEN ==="
    log "Total encontrados: $file_count"
    log "Procesados exitosamente: $PROCESSED"
    log "Fallidos: $FAILED"
    log "Omitidos: $SKIPPED"
    
    # Retornar código según resultado
    [[ $FAILED -eq 0 ]]
}

# =================== CLEANUP ===================
cleanup() {
    local exit_code=$?
	#Detiene el monitoreo en segundo plano
	[[ -n "${MONITOR_PID:-}" ]] && kill "$MONITOR_PID" 2>/dev/null || true

    flush_logs
    release_lock
    
if [[ $exit_code -eq 0 ]]; then
        log "=== Proceso finalizado exitosamente ==="
    else
        error "=== Proceso finalizado con errores (exit code: $exit_code) ==="
    fi
    
    exit $exit_code
}

# ==================== MONITORING =============
generate_metrics() {
    local ts=$(date +%s)
    cat <<EOF > "${LOG_DIR}/metrics.prom"
# HELP file_dispatcher_processed Archivos procesados exitosamente
# TYPE file_dispatcher_processed counter
file_dispatcher_processed ${PROCESSED}

# HELP file_dispatcher_failed Archivos fallidos
# TYPE file_dispatcher_failed counter
file_dispatcher_failed ${FAILED}

# HELP file_dispatcher_skipped Archivos omitidos
# TYPE file_dispatcher_skipped counter
file_dispatcher_skipped ${SKIPPED}

# HELP file_dispatcher_duration_seconds Duración total de ejecución
# TYPE file_dispatcher_duration_seconds gauge
file_dispatcher_duration_seconds ${duration}

# HELP file_dispatcher_timestamp Timestamp de ejecución
# TYPE file_dispatcher_timestamp gauge
file_dispatcher_timestamp ${ts}
EOF
}




# =================== MAIN ===================
main() {
	# para metricas y mediciones

	(
    while true; do
        echo "$(date +%T) CPU: $(top -bn1 | awk '/Cpu\(s\)/ {print $2}')% MEM: $(free -m | awk '/Mem:/ {print $3"MB"}')" >> "${LOG_DIR}/system_usag			e_$(date '+%Y%m%d').log"
        sleep 5
    	done
	) &
	MONITOR_PID=$!

	local start_time=$(date +%s)
    local start_mem=$(awk '/MemAvailable/ {print $2}' /proc/meminfo)
    local start_cpu=$(grep 'cpu ' /proc/stat | awk '{print $2+$4}')

    log "=== Iniciando File Dispatcher v2.1 con métricas ==="
    log "PID: $$ | Usuario: $(whoami) | Host: $(hostname)"
    log "RAM disponible inicial: ${start_mem} KB"

    initialize
    acquire_lock

    if search_and_process; then
        local end_time=$(date +%s)
        local end_mem=$(awk '/MemAvailable/ {print $2}' /proc/meminfo)
        local end_cpu=$(grep 'cpu ' /proc/stat | awk '{print $2+$4}')

        local duration=$((end_time - start_time))
        local mem_used=$((start_mem - end_mem))
        local cpu_used=$((end_cpu - start_cpu))

        log "=== MÉTRICAS ==="
        log "Duración total: ${duration}s"
        log "Memoria usada: ${mem_used} KB"
        log "CPU ticks usados: ${cpu_used}"
        log "Archivos procesados: ${PROCESSED}, fallidos: ${FAILED}, omitidos: ${SKIPPED}"
        log "Velocidad de procesamiento: $(bc <<< "scale=2; $PROCESSED / $duration") archivos/s"

        exit 0
    else
        exit 1
    fi
	generate_metrics
}

# Configurar traps
trap cleanup EXIT INT TERM HUP

# Ejecutar
main "$@"
