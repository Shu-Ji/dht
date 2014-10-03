ps aux | grep download_torrent.py | grep -v grep | awk '{print $2}' | xargs kill
