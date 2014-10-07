ps aux | grep hash_sync_day_job | grep -v grep | awk '{print $2}' | xargs kill
