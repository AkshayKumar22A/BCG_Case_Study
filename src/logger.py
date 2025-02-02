import logging
import sys

def configure_logger(name: str, log_level: str = "INFO") -> logging.Logger:
    """Configures a logger with dual handlers:
    - Console: Shows WARNING+ messages
    - File Handler: Logs all messages at specified level (DEBUG/INFO/etc.)
    
    Args:
        name: Logger name (usually __name__)
        log_level: Minimum level for file logging
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)  # Capture all levels
    
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Console handler shows only WARNING and above
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.WARNING)
    console_handler.setFormatter(formatter)
    
    # File handler uses configurable level
    file_handler = logging.FileHandler('case_study.log')
    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)
    
    if logger.hasHandlers():
        logger.handlers.clear()
        
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    return logger
