import requests
from auth import AuthAPIs
from datetime import datetime, timedelta

class GetAPIs:

    def __init__(self, repo_name, repo_owner):
        self.repo_name = repo_name
        self.repo_owner = repo_owner
        self.headers = AuthAPIs().create_connection()

    def get_contributors(self):
        url = f"https://api.github.com/repos/{self.repo_owner}/{self.repo_name}/contributors"
        response = requests.get(url, headers=self.headers)
        contributors = response.json()

        return {
            "total_developers": len(contributors)
        }

    def get_branches(self):
        url = f"https://api.github.com/repos/{self.repo_owner}/{self.repo_name}/branches"
        response = requests.get(url, headers=self.headers)
        branches = response.json()

        feature_branches = [
            b["name"] for b in branches if b["name"].startswith("feature")
        ]

        return {
            "total_branches": len(branches),
            "feature_branches": len(feature_branches)
        }

    def get_commits(self):
        url = f"https://api.github.com/repos/{self.repo_owner}/{self.repo_name}/commits"
        response = requests.get(url, headers=self.headers)
        commits = response.json()

        if not commits:
            return {
                "total_commits": 0,
                "last_commit_date": None
            }

        last_commit_date = commits[0]["commit"]["author"]["date"]

        return {
            "total_commits": len(commits),
            "last_commit_date": last_commit_date
        }

    def get_pulls(self):
        url = f"https://api.github.com/repos/{self.repo_owner}/{self.repo_name}/pulls"
        response = requests.get(url, headers=self.headers)
        pulls = response.json()

        return {
            "open_pull_requests": len(pulls)
        }
