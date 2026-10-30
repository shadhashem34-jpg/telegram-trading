import logging
import sys

class TradingLogger:
    def __init__(self, name="telegram_trading_bot"):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

    def info(self, msg, *args, **kwargs):
        self.logger.info(msg, *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        self.logger.error(msg, *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        self.logger.warning(msg, *args, **kwargs)

    def debug(self, msg, *args, **kwargs):
        self.logger.debug(msg, *args, **kwargs)

    def log_system_event(self, event, context=None):
        if context:
            self.logger.info(f"SYSTEM EVENT: {event} | Context: {context}")
        else:
            self.logger.info(f"SYSTEM EVENT: {event}")

    def log_error(self, error, context=None):
        if context:
            self.logger.error(f"ERROR: {error} | Context: {context}")
        else:
            self.logger.error(f"ERROR: {error}")

_logger_instance = None

def get_trading_logger():
    global _logger_instance
    if _logger_instance is None:
        _logger_instance = TradingLogger()
    return _logger_instance

def setup_logging():
    return get_trading_logger()
