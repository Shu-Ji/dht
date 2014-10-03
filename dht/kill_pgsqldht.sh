ps aux | grep pgsqldht.py | grep -v grep | awk '{print $2}' | xargs kill
