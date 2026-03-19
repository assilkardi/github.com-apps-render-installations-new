#!/bin/bash

echo "🚀 Lancement LeadGen..."

# Backend API
osascript -e 'tell app "Terminal" to do script "cd ~/bot_telegram_python && python3.11 -m uvicorn api_server:app --reload"'

# Bot Telegram
osascript -e 'tell app "Terminal" to do script "cd ~/bot_telegram_python && python3.11 main.py"'

# Frontend
osascript -e 'tell app "Terminal" to do script "cd ~/bot_telegram_python/leadgen-app && npm run dev"'