import logging

log_level = "INFO"
log_format = "%(asctime)s [%(levelname)s] %(message)s"

logging.root.handlers = []

logging.basicConfig(
    level=log_level,
    format=log_format,
    handlers=[
        logging.FileHandler("debug.log"),
#        logging.StreamHandler()
    ]
)


