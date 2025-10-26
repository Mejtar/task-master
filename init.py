#Este codigo es el encargado de generar los archivos que nutren de recursos la carpeta input del projecto 'job-simulator'
import os
import time
import re
import random
import argparse
import logging
import signal
from datetime import datetime

EXTENSIONS = [".payf", ".deb", ".transfer", ".credit", ".cash"]
MAX_SIZE = 5 * 1024
ERROR_PROBABILITY = 0.01
stop_requested = False


def _handle_signal(signum, frame):
    global stop_requested
    stop_requested = True


signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


def _ensure_dirs(base_path="recepcion"):
    input_path = os.path.join(base_path, "input")
    logs_path = os.path.join(base_path, "logs")
    os.makedirs(input_path, exist_ok=True)
    os.makedirs(logs_path, exist_ok=True)
    return input_path, logs_path


def _setup_logging(logs_path):
    metrics_file = os.path.join(logs_path, "metrics.log")
    errors_file = os.path.join(logs_path, "errors.log")

    logger = logging.getLogger("recepcion_metrics")
    logger.setLevel(logging.INFO)
    fh = logging.FileHandler(metrics_file)
    fh.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(fh)

    err_logger = logging.getLogger("recepcion_errors")
    err_logger.setLevel(logging.WARNING)
    efh = logging.FileHandler(errors_file)
    efh.setFormatter(logging.Formatter("%(message)s"))
    err_logger.addHandler(efh)

    if not os.path.exists(metrics_file) or os.stat(metrics_file).st_size == 0:
        logger.info("timestamp,filename,extension,size_bytes,elapsed_seconds")

    return logger, err_logger


def _parse_tiempo(tiempo_str):
    s = tiempo_str.lower().replace(" ", "")
    total_seconds = 0
    patterns = {"hr": 3600, "h": 3600, "min": 60, "m": 60}
    for key, mult in patterns.items():
        for v in re.findall(rf"(\d+)\s*{key}\b", s):
            total_seconds += int(v) * mult
    if total_seconds == 0:
        raise ValueError("Formato inválido: use '10min', '1hr 30m', etc.")
    return total_seconds


def _random_size(max_size):
    return random.randint(1, max_size) if max_size > 1 else 1


def _create_file(path, ext, size):
    timestamp = int(time.time() * 1000)
    pid = os.getpid()
    unique = f"{timestamp}-{pid}-{random.getrandbits(20)}"
    filename = f"{unique}{ext}"
    full = os.path.join(path, filename)
    with open(full, "wb") as f:
        f.write(os.urandom(size))
    return filename, full


def run_simulator(tiempo, cantidad, base_path="recepcion", loop=False):
    input_path, logs_path = _ensure_dirs(base_path)
    logger, err_logger = _setup_logging(logs_path)
    total_seconds = _parse_tiempo(tiempo)

    if cantidad <= 0:
        raise ValueError("cantidad must be > 0")

    interval = total_seconds / cantidad if cantidad > 0 else total_seconds
    interval = max(interval, 0)

    while True:
        start_cycle = time.monotonic()

        for i in range(cantidad):
            if stop_requested:
                return
            try:
                if random.random() < ERROR_PROBABILITY:
                    raise RuntimeError("Error simulado: fallo de escritura en disco")

                size = _random_size(MAX_SIZE)
                ext = random.choice(EXTENSIONS)
                filename, fullpath = _create_file(input_path, ext, size)
                elapsed = time.monotonic() - start_cycle
                ts = datetime.utcnow().isoformat() + "Z"
                logger.info(f"{ts},{filename},{ext},{size},{elapsed:.3f}")
            except Exception as e:
                ts = datetime.utcnow().isoformat() + "Z"
                err_logger.warning(f"{ts},error_creating_file,{i},{str(e)}")

            sleep_until = start_cycle + (i + 1) * interval
            to_sleep = sleep_until - time.monotonic()
            if to_sleep > 0:
                time.sleep(to_sleep)

            if stop_requested:
                return

        if not loop:
            break

        elapsed_cycle = time.monotonic() - start_cycle
        idle = max(0, total_seconds - elapsed_cycle)
        if idle > 0:
            time.sleep(idle)


def main():
    parser = argparse.ArgumentParser(description="Generador de archivos para el sistema de recepción de datos.")
    parser.add_argument("--tiempo", required=True, help="Duración total del ciclo (e.g. '1hr 30m', '45min')")
    parser.add_argument("--cantidad", required=True, type=int, help="Número de archivos a generar")
    parser.add_argument("--base-path", default="recepcion", help="Directorio base donde se guardarán input y logs")
    parser.add_argument("--loop", action="store_true", help="Repetir ejecución en bucle")
    args = parser.parse_args()
    run_simulator(args.tiempo, args.cantidad, base_path=args.base_path, loop=args.loop)


if __name__ == "__main__":
    main()
