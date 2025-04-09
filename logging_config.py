import logging
import os


def setup_logging(log_file='logs/app.log', log_level=logging.INFO):
    log_dir = os.path.dirname(log_file)
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    logging.basicConfig(
        filename=log_file,
        level=log_level,
        format='%(asctime)s - %(name)s - %(module)s - %(message)s',
        datefmt='%d-%m-%Y %H:%M:%S',
    )
    logger = logging.getLogger(__name__)
    return logger
