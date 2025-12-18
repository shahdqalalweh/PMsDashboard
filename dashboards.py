from getting import GetAPIs

class Dashboard:

    def __init__(self, repo_name, repo_owner):
        self.github = GetAPIs(repo_name, repo_owner)

    def show_dashboard(self):
        print("Project Dashboard\n")

        print(" Developers:")
        print(self.github.get_contributors())

        print("\n Branches:")
        print(self.github.get_branches())

        print("\n Commits:")
        print(self.github.get_commits())

        print("\n Pull Requests:")
        print(self.github.get_pulls())
