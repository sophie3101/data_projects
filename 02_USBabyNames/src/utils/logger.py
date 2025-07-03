import logging 
# def reset_logger(name):
#     logger = logging.getLogger(name)
#     # Remove and close all handlers
#     for handler in logger.handlers[:]:
#         logger.removeHandler(handler)
#         handler.close()

#     # Optional: reset level
#     logger.setLevel(logging.NOTSET)

#     # Optional: disable propagation
#     logger.propagate = False

#     return logger

def get_logger(name, log_file=None):
  logger = logging.getLogger(name) #returns a logger, __name__ is a special variable in Python that contains the name of the current module

  if logger.hasHandlers(): #if handlers are already added
    return logger 
  
  
  # logging.basicConfig(level=logging.DEBUG, format=format_)
  logger.setLevel(logging.DEBUG)
  format_ = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
  formatter = logging.Formatter(format_)
  """ set up file handler: INFO and above level is written to log file"""
  if log_file is not None:
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO) #minimum level of log messages
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
  """set up console handler: DEBUG and above level is written to console"""
  console_handler = logging.StreamHandler()
  console_handler.setLevel(logging.DEBUG)
  console_handler.setFormatter(formatter)
  logger.addHandler(console_handler)
  
  return logger