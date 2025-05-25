import logging
import sys

def get_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    """
    Initializes and returns a standardized logger.

    Args:
        name (str): The name for the logger (e.g., __name__ of the calling module).
        level (int): The logging level (e.g., logging.INFO, logging.DEBUG).

    Returns:
        logging.Logger: Configured logger instance.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Avoid adding multiple handlers if logger already has them
    if not logger.handlers:
        # Create a handler to log to stdout
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger

if __name__ == '__main__':
    # Example usage:
    logger = get_logger('my_app_logger', level=logging.DEBUG)
    logger.debug("This is a debug message.")
    logger.info("This is an info message.")
    logger.warning("This is a warning message.")
    logger.error("This is an error message.")
    logger.critical("This is a critical message.")

    # Example of getting the same logger instance
    another_logger = get_logger('my_app_logger')
    another_logger.info("This message comes from the same logger instance.")

    # Example of a different logger
    other_module_logger = get_logger('other_module', level=logging.INFO)
    other_module_logger.info("This is an info message from another logger.")
