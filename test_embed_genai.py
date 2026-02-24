import requests
import config

url = f"https://generativelanguage.googleapis.com/v1beta/models/text-embedding-004:batchEmbedContents?key={config.GOOGLE_API_KEY}"
res = requests.post(url, json={
    "requests": [{
        "model": "models/text-embedding-004",
        "content": {"parts": [{"text": "Hello"}]}
    }]
})
print("Result:", res.json())
