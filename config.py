import os
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv("TOKEN")
SERPAPI_KEY = os.getenv("SERPAPI_KEY")
SERPER_API_KEY = os.getenv("SERPER_API_KEY")