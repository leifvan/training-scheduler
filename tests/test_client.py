import unittest
import os
from time import sleep
from dataclasses import dataclass
from training_scheduler.config import trainingconfig
from training_scheduler.client import SchedulingClient
from training_scheduler.directory_adapters import LocalDirectoryAdapter


class TestSchedulingClient(unittest.TestCase):
    def setUp(self) -> None:
        data = """
                !trainingconfig/TestConfig
                test_string: Some string for testing.
                test_number: 3
                """
        planned_run_dir = os.path.join("test_dir", "planned_runs")
        os.makedirs(planned_run_dir)
        with open(os.path.join(planned_run_dir, 'test_config.yaml'), 'w') as file:
            file.write(data)

    def tearDown(self) -> None:
        os.remove(os.path.join("test_dir", "completed_runs", "test_config.yaml"))
        os.rmdir(os.path.join("test_dir", "completed_runs"))
        os.rmdir(os.path.join("test_dir", "active_runs"))
        os.rmdir(os.path.join("test_dir", "planned_runs"))
        os.rmdir("test_dir")

    def test_client_can_parse_correct_yaml_file(self):
        @trainingconfig
        @dataclass
        class TestConfig:
            test_string: str
            test_number: int

        sc = SchedulingClient(directory_adapter=LocalDirectoryAdapter("test_dir"),
                              timeout=10)

        def consumer(config):
            print("run")
            sleep(3)
            print("end")

        sc.register_config(config_class=TestConfig, consumer_fn=consumer)
        sc.run()

        self.assertTrue(os.path.isfile(os.path.join("test_dir","completed_runs","test_config.yaml")))


if __name__ == '__main__':
    unittest.main()
