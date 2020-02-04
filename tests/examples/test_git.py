import unittest, os, shutil
from training_scheduler.client import SchedulingClient
from training_scheduler.directory_adapters import LocalDirectoryAdapter
from training_scheduler.examples.git import GitPullConfig, git_pull_consumer


class GitExampleTests(unittest.TestCase):
    def setUp(self) -> None:
        data = '!trainingconfig/GitPullConfig\nrepo_path: "."\n'

        planned_run_dir = os.path.join("test_dir", "planned_runs")
        os.makedirs(planned_run_dir)
        with open(os.path.join(planned_run_dir, 'test_config.yaml'), 'w') as file:
            file.write(data)

    def tearDown(self) -> None:
        shutil.rmtree("test_dir")

    def test_git_pull(self):
        sc = SchedulingClient(directory_adapter=LocalDirectoryAdapter("test_dir"),
                              timeout=10)
        sc.register_config(GitPullConfig, git_pull_consumer)
        sc.run()


if __name__ == '__main__':
    unittest.main()
