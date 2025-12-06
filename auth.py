import os

class AuthAPIs:

    def create_connection(self):
        token = os.getenv("GITHUB_TOKEN")

        if not token:
            raise Exception("GITHUB_TOKEN is not set in environment variables")

        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
        }
        return headers
