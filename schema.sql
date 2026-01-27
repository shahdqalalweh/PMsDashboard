CREATE TABLE IF NOT EXISTS repo (
    repo_id BIGINT PRIMARY KEY,
    owner TEXT NOT NULL,
    name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS branches (
    branch_id SERIAL PRIMARY KEY,
    repo_id BIGINT REFERENCES repo(repo_id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    sha TEXT NOT NULL,
    is_main BOOLEAN NOT NULL DEFAULT FALSE,
    UNIQUE (repo_id, name)
);


CREATE TABLE IF NOT EXISTS commits (
    commit_id SERIAL PRIMARY KEY,
    repo_id BIGINT REFERENCES repo(repo_id) ON DELETE CASCADE,
    sha TEXT NOT NULL,
    committed_at TIMESTAMP NOT NULL,
    UNIQUE (repo_id, sha)
);

CREATE TABLE IF NOT EXISTS pulls (
    pull_id BIGINT PRIMARY KEY,
    repo_id BIGINT REFERENCES repo(repo_id) ON DELETE CASCADE,
    merged BOOLEAN,
    created_at TIMESTAMP,
    merged_at TIMESTAMP
);
