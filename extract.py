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


@task(retries=3, retry_delay_seconds=5)
def extract_repository(owner: str, repo: str):
    """Fetch repository metadata (id, default branch, etc.)."""
    url = f"{BASE}/repos/{owner}/{repo}"
    headers = AuthAPIs().create_connection()
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    return r.json()


@task(retries=3, retry_delay_seconds=5)
def extract_branch_commits(owner: str, repo: str, branch_name: str, since_ts=None, max_items: int = 500):
    """Return commits reachable from a branch (optionally since a timestamp).

    Notes:
    - GitHub's /commits without sha defaults to the default branch.
    - To map commits to branches we need /commits?sha=<branch>.
    - max_items protects you from accidentally paging forever on large repos.
    """
    url = repo_url(owner, repo, "commits")
    headers = AuthAPIs().create_connection()
    params = {"sha": branch_name}
    if since_ts:
        if since_ts.tzinfo is None:
            since_ts = since_ts.replace(tzinfo=timezone.utc)
        params["since"] = since_ts.isoformat()

    out = []
    for item in paginate(url, headers, params=params):
        out.append(item)
        if len(out) >= max_items:
            break
    return out

"""
paginate()

يجيب صفحة صفحة page=1,2,3...

per_page=100
page=1: يجيب أول 100 عنصر

page=2: ثاني 100
يوقف لما يرجع []
"""
def paginate(url, headers, params=None):
    params = params or {} #dic
    #dictionary فيها query parameters إضافية
    page = 1
    while True:  # ما بعرف كم صفحة
        p = {**params, "per_page": 100, "page": page}
        # اعمل دكشنري جديد وانسخ كل اللي في البارمز وبعدين ضيف او استبدل البيج والبير بيج
        #dictionary unpacking
        r = requests.get(url, headers=headers, params=p, timeout=30)
        r.raise_for_status() # اكسبشن
        data = r.json() # convert to json
        if not data:
            break
        for item in data: #generator
            yield item
        page += 1



@task(retries=3, retry_delay_seconds=5)
def extract_contributors(owner: str, repo: str):
    url = repo_url(owner, repo, "contributors")
    headers = AuthAPIs().create_connection() #auth
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
    if since_ts: #رجّع commits اللي بعد هذا التاريخ
        # نخليها UTC
        if since_ts.tzinfo is None:
            since_ts = since_ts.replace(tzinfo=timezone.utc)
        params["since"] = since_ts.isoformat() #ISO مناسب للAPI
    return list(paginate(url, headers, params=params))

#ما في since مباشر مثل commits

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
                if pr_updated < since_utc: #لما يصير أقدم من since_ts
                    break
        pulls.append(pr) #اذا لسا ضمن الفترة

    return pulls


"""
contributors = list[dict]

branches = list[dict]

commits = list[dict]

pulls = list[dict]

"""


"""

params = {"since": "2026-01-03"}
p = {**params}

p = {"since": "2026-01-03"}

يعني p نسخة من params.







params = {"since": "2026-01-03T15:00:00+00:00"}
page = 2


p = {**params, "per_page": 100, "page": page}
يصير:

p = {
  "since": "2026-01-03T15:00:00+00:00",
  "per_page": 100,
  "page": 2
}
"""