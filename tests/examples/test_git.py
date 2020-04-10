import unittest, os, shutil
from training_scheduler.client import SchedulingClient
from training_scheduler.directory_adapters import LocalDirectoryAdapter
from training_scheduler.examples.git import GitPullConfig, git_pull_consumer, GitRepoDirtyException


class GitExampleTests(unittest.TestCase):
    def setUp(self) -> None:
        data = '!trainingconfig/GitPullConfig\nrepo_path: ".."\n'

        planned_run_dir = os.path.join("test_dir", "planned")
        os.makedirs(planned_run_dir)
        with open(os.path.join(planned_run_dir, 'test_config.yaml'), 'w') as file:
            file.write(data)

    def tearDown(self) -> None:
        shutil.rmtree("test_dir")

    def test_git_pull(self):
        # TODO make this self-contained. Currently it does not work with tox because there is no git
        sc = SchedulingClient(directory_adapter=LocalDirectoryAdapter("test_dir"),
                              timeout=10)
        sc.register_config(GitPullConfig, git_pull_consumer)
        try:
            sc.run(debug=True)
        except GitRepoDirtyException:
            pass


if __name__ == '__main__':
    unittest.main()
