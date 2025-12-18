import requests
from auth import AuthAPIs
from datetime import datetime, timedelta

class GetAPIs:
    # Handles GitHub API requests and returns filtered metrics for dashboard

    def __init__(self, repo_name, repo_owner):
        self.repo_name = repo_name
        self.repo_owner = repo_owner
        self.headers = AuthAPIs().create_connection()  # Authenticate once

    def get_url(self, nameofendpoint):
    # Helper to build GitHub API URLs for this repo
        return f"https://api.github.com/repos/{self.repo_owner}/{self.repo_name}/{nameofendpoint}"

    def get_contributors(self):
        # Return total number of developers
        url = self.get_url("contributors")
        response = requests.get(url, headers=self.headers)
        contributors = response.json()
        return {"total_developers": len(contributors)}

    def get_branches(self):
        # Return total branches and count of feature branches
        url = self.get_url("branches")
        response = requests.get(url, headers=self.headers)
        branches = response.json()
        feature_branches = [b["name"] for b in branches if b["name"].startswith("feature")]
        return {"total_branches": len(branches), "feature_branches": len(feature_branches)}

    def get_commits(self):
        # Return total commits and date of last commit
        url = self.get_url("commits")
        response = requests.get(url, headers=self.headers)
        commits = response.json()
        if not commits:
            return {"total_commits": 0, "last_commit_date": None}
        last_commit_date = commits[0]["commit"]["author"]["date"]
        return {"total_commits": len(commits), "last_commit_date": last_commit_date}

    def get_pulls(self):
        # Return number of open pull requests
        url = self.get_url("pulls")
        response = requests.get(url, headers=self.headers)
        pulls = response.json()
        return {"open_pull_requests": len(pulls)}
