import logging
import sys
from datetime import datetime

# Konfigurasi Logging Dasar
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

def _get_timestamp():
    return datetime.now().strftime("%H:%M:%S")

def info(message):
    """Mencetak pesan INFO (Warna Putih/Normal)"""
    print(f"[{_get_timestamp()}] ℹ️  {message}")

def warn(message):
    """Mencetak pesan WARNING (Warna Kuning)"""
    # Kode warna ANSI Yellow: \033[93m
    print(f"[{_get_timestamp()}] ⚠️  \033[93m{message}\033[0m")

def error(message):
    """Mencetak pesan ERROR (Warna Merah)"""
    # Kode warna ANSI Red: \033[91m
    print(f"[{_get_timestamp()}] ❌ \033[91m{message}\033[0m")