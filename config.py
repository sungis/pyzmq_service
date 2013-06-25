MONGO_CONN = 'mongodb://10.0.1.76:19753'
SKILL_FILE = './dict/skill.txt'

HEARTBEAT_LIVENESS = 5     # 3..5 is reasonable
HEARTBEAT_INTERVAL = 1   # Seconds

INTERVAL_INIT = 1
INTERVAL_MAX = 32

#  Paranoid Pirate Protocol constants
PPP_READY = "\x01"      # Signals worker is ready
PPP_HEARTBEAT = "\x02"  # Signals worker heartbeat

import logging
def getLogger(name,logfile):
    logger=logging.getLogger(name)
    handler=logging.FileHandler(logfile)
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger
