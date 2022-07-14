#!/bin/bash

# Change directory to the app folder
cd ~/vacquishcovid19_postgres/covid_app

# Kill the dash app
killall screen
# Restart the dash app
screen -d -m python3 app.py
