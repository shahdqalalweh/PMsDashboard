from prefect import flow
from extract import (
    get_contributors,
    get_branches,
    get_commits,
    get_pulls
)
from load import load_metrics

@flow(log_prints=True)
def github_dashboard(repo_name, repo_owner):

    devs = get_contributors(repo_owner, repo_name)
    branches = get_branches(repo_owner, repo_name)
    commits = get_commits(repo_owner, repo_name)
    pulls = get_pulls(repo_owner, repo_name)

    load_metrics(devs, branches, commits, pulls)
    print("Pipeline finished successfully")
