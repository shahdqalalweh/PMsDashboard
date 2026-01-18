-- Minimal schema (PostgreSQL) matching the corrected ERD + practical constraints.

CREATE TABLE IF NOT EXISTS employees (
  employee_id INT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS github_users (
  github_user_id BIGINT PRIMARY KEY,
  github_login TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS employee_github_accounts (
  employee_id INT NOT NULL,
  github_user_id BIGINT NOT NULL,
  PRIMARY KEY (employee_id, github_user_id),
  FOREIGN KEY (employee_id) REFERENCES employees(employee_id) ON DELETE CASCADE,
  FOREIGN KEY (github_user_id) REFERENCES github_users(github_user_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS repositories (
  repo_id BIGINT PRIMARY KEY,
  owner_org TEXT NOT NULL,
  name TEXT NOT NULL,
  default_branch_name TEXT NOT NULL,
  default_branch_repo_id BIGINT NULL,
  default_branch_branch_name TEXT NULL,
  CONSTRAINT uq_repo_owner_name UNIQUE (owner_org, name)
);

CREATE TABLE IF NOT EXISTS branches (
  repo_id BIGINT NOT NULL,
  name TEXT NOT NULL,
  first_seen_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (repo_id, name),
  FOREIGN KEY (repo_id) REFERENCES repositories(repo_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS commits (
  repo_id BIGINT NOT NULL,
  sha TEXT NOT NULL,
  committed_at TIMESTAMPTZ NOT NULL,
  author_github_user_id BIGINT NULL,
  PRIMARY KEY (repo_id, sha),
  FOREIGN KEY (repo_id) REFERENCES repositories(repo_id) ON DELETE CASCADE,
  FOREIGN KEY (author_github_user_id) REFERENCES github_users(github_user_id)
);

CREATE TABLE IF NOT EXISTS branch_commits (
  repo_id BIGINT NOT NULL,
  branch_name TEXT NOT NULL,
  sha TEXT NOT NULL,
  PRIMARY KEY (repo_id, branch_name, sha),
  FOREIGN KEY (repo_id, branch_name) REFERENCES branches(repo_id, name) ON DELETE CASCADE,
  FOREIGN KEY (repo_id, sha) REFERENCES commits(repo_id, sha) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS pull_requests (
  repo_id BIGINT NOT NULL,
  pr_number INT NOT NULL,
  pr_id BIGINT NOT NULL, -- REST pulls "id" (int). إذا بدك GraphQL node_id خليه TEXT.
  author_github_user_id BIGINT NULL,
  state TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  merged_at TIMESTAMPTZ NULL,

  head_repo_id BIGINT NULL,
  head_ref TEXT NULL,
  base_repo_id BIGINT NULL,
  base_ref TEXT NULL,

  PRIMARY KEY (repo_id, pr_number),
  FOREIGN KEY (repo_id) REFERENCES repositories(repo_id) ON DELETE CASCADE,
  FOREIGN KEY (author_github_user_id) REFERENCES github_users(github_user_id),
  FOREIGN KEY (head_repo_id) REFERENCES repositories(repo_id),
  FOREIGN KEY (base_repo_id) REFERENCES repositories(repo_id)
);

CREATE INDEX IF NOT EXISTS idx_commits_repo_committed_at ON commits(repo_id, committed_at);
CREATE INDEX IF NOT EXISTS idx_pulls_repo_created_at ON pull_requests(repo_id, created_at);
