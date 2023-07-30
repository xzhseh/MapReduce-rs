# Use ps command to find processes by name and kill them
kill $(ps aux | grep '[m]rcoordinator' | awk '{print $2}')
kill $(ps aux | grep '[m]rworker' | awk '{print $2}')
