import asyncio
import aiohttp
import config
import base64
import json

async def test():
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{config.GEMINI_VISION_MODEL}:generateContent?key={config.GOOGLE_API_KEY}"
    
    payload = {
        "contents": [{
            "parts": [{"text": "Hello"}]
        }]
    }
    
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload) as resp:
            print("Status:", resp.status)
            print(await resp.text())

asyncio.run(test())
