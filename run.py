from dotenv import load_dotenv
load_dotenv()

from flow import github_etl

if __name__ == "__main__":
    github_etl("shahdqalalweh", "Software-Engineering")
