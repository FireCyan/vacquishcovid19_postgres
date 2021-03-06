#!/bin/bash

# Change directory to JohnHopkins github repo
cd ~/covid19-project

# Run daily_update_script.py
python3 daily_update_script.py

# Sleep for 10 seconds
sleep 10
# Kill the dash app
killall screen

# Change directory to the app folder
cd ~/covid19-project/covid_app

# Restart the dash app
screen -d -m python3 app.py