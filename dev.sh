#!/bin/bash

echo "🔥 MODE DEV - LeadGen"

# API
osascript -e 'tell app "Terminal" to do script "cd ~/bot_telegram_python && python3.11 -m uvicorn api_server:app --reload"'

# BOT
osascript -e 'tell app "Terminal" to do script "cd ~/bot_telegram_python && python3.11 main.py"'

# FRONT
osascript -e 'tell app "Terminal" to do script "cd ~/bot_telegram_python/leadgen-app && npm run dev"'