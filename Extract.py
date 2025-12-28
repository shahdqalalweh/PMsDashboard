import requests
from prefect import task
from auth import AuthAPIs

class GetAPIs:

    def __init__(self, repo_name, repo_owner):
        self.repo_name = repo_name
        self.repo_owner = repo_owner
        self.headers = AuthAPIs().create_connection()

    def get_url(self, endpoint):
        return f"https://api.github.com/repos/{self.repo_owner}/{self.repo_name}/{endpoint}"

    async def fetch(self, session, endpoint):
        async with session.get(self.get_url(endpoint), headers=self.headers) as response:
            return await response.json()

    @task(retries=3, retry_delay_seconds=5)
    async def get_contributors(self):
        async with aiohttp.ClientSession() as session:
            data = await self.fetch(session, "contributors")
            return {"total_developers": len(data)}

    @task(retries=3, retry_delay_seconds=5)
    async def get_branches(self):
        async with aiohttp.ClientSession() as session:
            data = await self.fetch(session, "branches")
            feature = [b["name"] for b in data if b["name"].startswith("feature")]
            return {
                "total_branches": len(data),
                "feature_branches": len(feature)
            }

    @task(retries=3, retry_delay_seconds=5)
    async def get_commits(self):
        async with aiohttp.ClientSession() as session:
            data = await self.fetch(session, "commits")
            if not data:
                return {"total_commits": 0, "last_commit_date": None}

            return {
                "total_commits": len(data),
                "last_commit_date": data[0]["commit"]["author"]["date"]
            }

    @task(retries=3, retry_delay_seconds=5)
    async def get_pulls(self):
        async with aiohttp.ClientSession() as session:
            data = await self.fetch(session, "pulls")
            return {"open_pull_requests": len(data)}
