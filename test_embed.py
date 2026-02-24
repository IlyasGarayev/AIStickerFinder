import asyncio
import aiohttp
import config

async def test():
    # The new embed model is text-embedding-004-preview ? No, the API says "not found". Let's check what models exist.
    url = f"https://generativelanguage.googleapis.com/v1beta/models?key={config.GOOGLE_API_KEY}"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            data = await resp.json()
            for m in data.get("models", []):
                if "embed" in m["name"]:
                    print(m["name"])

asyncio.run(test())
