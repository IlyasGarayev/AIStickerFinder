import requests
url = "https://aistickerfinder-git-91281591862.europe-west1.run.app/health"
try:
    print(requests.get(url).text)
except Exception as e:
    print("Error:", e)
