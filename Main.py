from dashboards import Dashboard

if __name__ == "__main__":
    repo_name = "Software-Engineering"
    repo_owner = "shahdqalalweh"

    dash = Dashboard(repo_name, repo_owner)
    dash.show_dashboard()
