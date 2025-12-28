from prefect import flow
from Extract import GetAPIs

class Dashboard:

    def __init__(self, repo_name, repo_owner):
        self.github = GetAPIs(repo_name, repo_owner)

    @flow(log_prints=True)
    def show_dashboard(self):
        print("Project Dashboard\n")

        devs = self.github.get_contributors()
        branches = self.github.get_branches()
        commits = self.github.get_commits()
        pulls = self.github.get_pulls()

        print("Developers:", devs)
        print("Branches:", branches)
        print("Commits:", commits)
        print("Pull Requests:", pulls)
