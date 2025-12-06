import requests
from auth import AuthAPIs

class GetAPIs:

    def __init__(self, repo_name, repo_owner):
        self.repo_name = repo_name
        self.repo_owner = repo_owner
        self.headers = AuthAPIs().create_connection()

    def get_contributors(self):
        url = f"https://api.github.com/repos/{self.repo_owner}/{self.repo_name}/contributors"
        response = requests.get(url, headers=self.headers)
        return response.json()

    def get_branches(self):
        url = f"https://api.github.com/repos/{self.repo_owner}/{self.repo_name}/branches"
        response = requests.get(url, headers=self.headers)
        return response.json()

    def get_commits(self):
        url = f"https://api.github.com/repos/{self.repo_owner}/{self.repo_name}/commits"
        response = requests.get(url, headers=self.headers)
        return response.json()

    def get_pulls(self):
        url = f"https://api.github.com/repos/{self.repo_owner}/{self.repo_name}/pulls"
        response = requests.get(url, headers=self.headers)
        return response.json()
