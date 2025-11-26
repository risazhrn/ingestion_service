import logging
import sys
from datetime import datetime

# --- Custom Formatter with Colors ---
class ColorFormatter(logging.Formatter):
    COLORS = {
        'INFO': '\033[0m',       # white
        'WARNING': '\033[93m',   # yellow
        'ERROR': '\033[91m',     # red
    }
    RESET = '\033[0m'

    def format(self, record):
        timestamp = datetime.now().strftime("%H:%M:%S")
        emoji = {
            'INFO': "ℹ️ ",
            'WARNING': "⚠️ ",
            'ERROR': "❌",
        }.get(record.levelname, "")

        color = self.COLORS.get(record.levelname, "")
        message = super().format(record)

        return f"[{timestamp}] {emoji} {color}{message}{self.RESET}"


# --- Setup Logger ---
logger = logging.getLogger("omnichannel")
logger.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(ColorFormatter("%(message)s"))
logger.addHandler(handler)

# Public wrappers
def info(msg):
    logger.info(msg)

def warn(msg):
    logger.warning(msg)

def error(msg):
    logger.error(msg)
