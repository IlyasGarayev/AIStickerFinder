import requests
import json
url = "https://aistickerfinder-git-91281591862.europe-west1.run.app/health"
res = requests.get(url)
print("Health:", res.status_code, res.text)
url_search = "https://aistickerfinder-git-91281591862.europe-west1.run.app/search"
res2 = requests.post(url_search, json={"query": "test"})
print("Search:", res2.status_code, res2.text)
