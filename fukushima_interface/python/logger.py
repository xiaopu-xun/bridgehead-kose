import sys
from logging import getLogger, StreamHandler, INFO

# ロガー作成
logger = getLogger()
logger.addHandler(StreamHandler(sys.stdout))
logger.setLevel(INFO)
