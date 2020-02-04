import unittest
import os
import shutil
from typing import Optional
from time import sleep
from dataclasses import dataclass
from training_scheduler.config import trainingconfig
from training_scheduler.client import SchedulingClient
from training_scheduler.directory_adapters import LocalDirectoryAdapter


@trainingconfig
@dataclass
class _TestConfig:
    test_string: Optional[str] = None


def _consumer(config: _TestConfig):
    print("run")
    sleep(3)
    print("end")
    if config.test_string is not None:
        return config.test_string


class TestSchedulingClient(unittest.TestCase):
    def setUp(self) -> None:
        self.planned_run_dir = os.path.join("test_dir", "planned")
        os.makedirs(self.planned_run_dir)

        self.sc = SchedulingClient(directory_adapter=LocalDirectoryAdapter("test_dir"),
                                   timeout=5)

        self.sc.register_config(config_class=_TestConfig, consumer_fn=_consumer)

    def tearDown(self) -> None:
        shutil.rmtree("test_dir")

    def test_client_parses_config_without_output(self):
        data = "!trainingconfig/_TestConfig\ntest_string: null\n"

        with open(os.path.join(self.planned_run_dir, 'test_config_empty.yaml'), 'w') as file:
            file.write(data)

        self.sc.run()
        self.assertTrue(os.path.isfile(os.path.join("test_dir",
                                                    "completed",
                                                    "test_config_empty.yaml")))
        self.assertFalse(os.path.isfile(os.path.join("test_dir",
                                                     "completed",
                                                     "test_config_empty.yaml.out")))

    def test_client_parses_config_with_output(self):
        data = "!trainingconfig/_TestConfig\ntest_string: Some string for testing.\n"

        with open(os.path.join(self.planned_run_dir, 'test_config_output.yaml'), 'w') as file:
            file.write(data)

        self.sc.run()
        self.assertTrue(os.path.isfile(os.path.join("test_dir",
                                                    "completed",
                                                    "test_config_output.yaml")))

        self.assertTrue(os.path.isfile(os.path.join("test_dir",
                                                    "completed",
                                                    "test_config_output.yaml.out")))


if __name__ == '__main__':
    unittest.main()
