import logging
import json

class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_data = {
            'level': record.levelname,
            'message': record.getMessage(),
            'timestamp': self.formatTime(record, self.datefmt),
            'payload': getattr(record, 'payload', '')  # Usando getattr para pegar o atributo 'payload' do registro
            # Adicione outros campos que deseja incluir no log JSON
        }
        return json.dumps(log_data)

class CustomLogger(logging.Logger):
    def __init__(self, name, level=logging.NOTSET):
        super().__init__(name, level)

    def log(self, level, msg, *args, **kwargs):
        if 'payload' in kwargs:
            setattr(self, 'payload', kwargs['payload'])
        super().log(level, msg, *args)

def setup_logger(logger_name, level=logging.INFO):
    logger = CustomLogger(logger_name, level)
    logger.setLevel(level)
    formatter = JsonFormatter()

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger