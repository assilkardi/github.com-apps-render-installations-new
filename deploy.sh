#!/bin/bash

echo "🚀 DEPLOY EN COURS..."

cd ~/bot_telegram_python

git add .
git commit -m "update auto"
git push

echo "✅ Deploy terminé"