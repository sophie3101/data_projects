import logging 

def get_logger(name, log_file=None):
  logger = logging.getLogger(name) #returns a logger, __name__ is a special variable in Python that contains the name of the current module
  logger.setLevel(logging.DEBUG)
  if logger.hasHandlers(): #if handlers are already added
    return logger 
  
  format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
  """ set up file handler: INFO and above level is written to log file"""
  if log_file is not None:
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO) #minimum level of log messages
    file_handler.setFormatter(format)
    logger.addHandler(file_handler)
  """set up console handler: DEBUG and above level is written to console"""
  console_handler = logging.StreamHandler()
  console_handler.setLevel(logging.DEBUG)
  console_handler.setFormatter(format)
  logger.addHandler(console_handler)
  
  return logger