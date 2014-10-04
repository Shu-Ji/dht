import datetime
import time
import os


while 1:
    print 'start', datetime.datetime.now().strftime('%y-%m-%d %H:%M:%S')
    os.system('python tor_parser.py')
    time.sleep(60 * 3)
