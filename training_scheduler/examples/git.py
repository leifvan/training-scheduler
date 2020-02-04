from git import Repo
from dataclasses import dataclass
from ..config import trainingconfig

# TODO consider https://github.com/gitpython-developers/GitPython#leakage-of-system-resources


@trainingconfig
@dataclass
class GitPullConfig:
    repo_path: str = "."


def git_pull_consumer(config: GitPullConfig):
    repo = Repo(config.repo_path)
    if not repo.is_dirty():
        repo.remotes.origin.pull()
        print("pulled")
    else:
        raise Exception("Repo is dirty!")
