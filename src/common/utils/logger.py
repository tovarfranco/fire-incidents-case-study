import logging
    
def init_logging(
    level: str = "INFO",
    msg_format: str = "%(asctime)s %(levelname)s %(name)s %(funcName)s: %(message)s",
    datetime_format: str = "%Y-%m-%d %H:%M:%S",
) -> logging.Logger:
    """Init logging settings

    Parameters
    ----------
    level : str
        Logging level, by default INFO
    msg_format : str, optional
        Logging msg format, by default "%(asctime)s %(levelname)s %(name)s %(funcName)s: %(message)s"
    datetime_format : str, optional
        Datetime format logger, by default "%Y-%m-%d %H:%M:%S"

    Returns
    -------
    Logger
        logger instance
    """

    logging.basicConfig(format=msg_format, datefmt=datetime_format)
    logger = logging.getLogger()
    logger.setLevel(level)

    return logger

def init_logging_2(name):
    """Init logging settings

    Args:
        name (str): Logger name

    Returns:
        logger: logger instance
    """
    MSG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"
    DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

    logging.basicConfig(format=MSG_FORMAT, datefmt=DATETIME_FORMAT)
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    return logger
 