import requests
from prefect import task
from datetime import datetime, timezone
from auth import AuthAPIs

"""
pagination
gitub return the first 30
ع شان هيك لازم نعمل ال Pagination  لحتى يرجع الكل
"""

BASE = "https://api.github.com"

def repo_url(owner, repo, endpoint):
    return f"{BASE}/repos/{owner}/{repo}/{endpoint}"

"""
paginate()

يجيب صفحة صفحة page=1,2,3...

per_page=100 (الأقصى تقريباً)

يوقف لما يرجع []
"""
def paginate(url, headers, params=None):
    params = params or {}
    page = 1
    while True:
        p = {**params, "per_page": 100, "page": page}
        r = requests.get(url, headers=headers, params=p, timeout=30)
        r.raise_for_status()
        data = r.json()
        if not data:
            break
        for item in data:
            yield item
        page += 1



@task(retries=3, retry_delay_seconds=5)
def extract_contributors(owner: str, repo: str):
    url = repo_url(owner, repo, "contributors")
    headers = AuthAPIs().create_connection()
    return list(paginate(url, headers))

@task(retries=3, retry_delay_seconds=5)
def extract_branches(owner: str, repo: str):
    url = repo_url(owner, repo, "branches")
    headers = AuthAPIs().create_connection()
    return list(paginate(url, headers))


"""
فيرجع commits الجديدة فقط.
"""
@task(retries=3, retry_delay_seconds=5)
def extract_commits(owner: str, repo: str, since_ts):
    url = repo_url(owner, repo, "commits")
    headers = AuthAPIs().create_connection()
    params = {}
    if since_ts:
        # نخليها UTC
        if since_ts.tzinfo is None:
            since_ts = since_ts.replace(tzinfo=timezone.utc)
        params["since"] = since_ts.isoformat()
    return list(paginate(url, headers, params=params))

@task(retries=3, retry_delay_seconds=5)
def extract_pulls(owner: str, repo: str, since_ts):
    url = repo_url(owner, repo, "pulls")
    headers = AuthAPIs().create_connection()
    params = {"state": "all", "sort": "updated", "direction": "desc"}

    pulls = []
    since_utc = None
    if since_ts:
        since_utc = since_ts.replace(tzinfo=timezone.utc) if since_ts.tzinfo is None else since_ts.astimezone(timezone.utc)

    for pr in paginate(url, headers, params=params):
        if since_utc:
            updated_at = pr.get("updated_at")
            if updated_at:
                pr_updated = datetime.fromisoformat(updated_at.replace("Z", "+00:00"))
                if pr_updated < since_utc:
                    break
        pulls.append(pr)

    return pulls
