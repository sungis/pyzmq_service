MONGO_CONN = 'mongodb://10.0.1.76:19753'
SKILL_FILE = './dict/skill.txt'

HEARTBEAT_LIVENESS = 5     # 3..5 is reasonable
HEARTBEAT_INTERVAL = 1   # Seconds

INTERVAL_INIT = 1
INTERVAL_MAX = 32

#  Paranoid Pirate Protocol constants
PPP_READY = "\x01"      # Signals worker is ready
PPP_HEARTBEAT = "\x02"  # Signals worker heartbeat

