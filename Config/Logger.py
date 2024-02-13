import logging
import colorlog

# ----------------------------- LOGGING CONFIGURATION -----------------------------#
color_map = {
    "DEBUG": "green",
    "INFO": "blue",
    "WARNING": "yellow",
    "ERROR": "red",
    "CRITICAL": "bold_red",
}

formatter = colorlog.ColoredFormatter(
    "%(log_color)s%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    log_colors=color_map,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

LOGGER = logging.getLogger(__name__)

handler = logging.StreamHandler()
handler.setFormatter(formatter)
LOGGER.addHandler(handler)
