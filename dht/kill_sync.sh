ps aux | grep hash_sync.py | grep -v grep | awk '{print $2}' | xargs kill
