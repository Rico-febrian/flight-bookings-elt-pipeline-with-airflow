import logging

def logger(logger_name: str):
    # Create logger instance
    log = logging.getLogger(logger_name)
    log.setLevel(logging.INFO)
    
    # Create stream handler (console)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    
    # Simple log format
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    
    if not log.hasHandlers():
        log.addHandler(ch)
    
    return log
