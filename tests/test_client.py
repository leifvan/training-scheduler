import unittest
import os
import shutil
from typing import Optional
from time import sleep
from dataclasses import dataclass
from training_scheduler.config import trainingconfig
from training_scheduler.client import SchedulingClient
from training_scheduler.directory_adapters import LocalDirectoryAdapter


class TestDifferentCallbacksInClient(unittest.TestCase):
    def setUp(self) -> None:
        os.makedirs("test_dir")

    def tearDown(self) -> None:
        shutil.rmtree("test_dir")

    def test_if_client_can_be_created_with_None_as_callback(self):
        self.sc = SchedulingClient(directory_adapter=LocalDirectoryAdapter("test_dir"),
                                   callback=None, timeout=5)
        self.sc.run()
        self.assertTrue(True)


class TestSchedulingClient(unittest.TestCase):
    def setUp(self) -> None:
        self.planned_run_dir = os.path.join("test_dir", "planned")
        os.makedirs(self.planned_run_dir)

        self.sc = SchedulingClient(directory_adapter=LocalDirectoryAdapter("test_dir"),
                                   timeout=5)

    def tearDown(self) -> None:
        shutil.rmtree("test_dir")

    def test_client_parses_config_without_output(self):
        @trainingconfig
        @dataclass
        class TestConfigEmpty:
            test_string: Optional[str] = None

        def consumer(config: TestConfigEmpty, identifier: str):
            self.assertEqual(type(config), TestConfigEmpty)
            print("run")
            sleep(3)
            print("end")

        self.sc.register_config(config_class=TestConfigEmpty, consumer_fn=consumer)

        data = "!trainingconfig/TestConfigEmpty\ntest_string: null\n"

        with open(os.path.join(self.planned_run_dir, 'test_config_empty.yaml'), 'w') as file:
            file.write(data)

        self.sc.run(debug=True)
        self.assertTrue(os.path.isfile(os.path.join("test_dir",
                                                    "completed",
                                                    "test_config_empty.yaml")))
        self.assertFalse(os.path.isfile(os.path.join("test_dir",
                                                     "completed",
                                                     "test_config_empty.yaml.out")))

    def test_client_parses_config_with_output(self):
        @trainingconfig
        @dataclass
        class TestConfigOutput:
            test_string: Optional[str] = None

        def consumer(config: TestConfigOutput, identifier: str):
            print("run")
            sleep(3)
            print("end")
            return config.test_string

        self.sc.register_config(config_class=TestConfigOutput, consumer_fn=consumer)

        data = "!trainingconfig/TestConfigOutput\ntest_string: Some string for testing.\n"

        with open(os.path.join(self.planned_run_dir, 'test_config_output.yaml'), 'w') as file:
            file.write(data)

        self.sc.run(debug=True)
        self.assertTrue(os.path.isfile(os.path.join("test_dir",
                                                    "completed",
                                                    "test_config_output.yaml")))

        self.assertTrue(os.path.isfile(os.path.join("test_dir",
                                                    "completed",
                                                    "test_config_output.yaml.out")))

    def test_if_exception_is_reraised_in_debug_mode(self):
        @trainingconfig
        @dataclass
        class TestConfigForDebug:
            test_string: Optional[str] = None

        def consumer(config: TestConfigForDebug, identifier: str):
            print("run")
            sleep(3)
            print("raise")
            raise Exception("Some test problem.")

        self.sc.register_config(config_class=TestConfigForDebug, consumer_fn=consumer)

        data = "!trainingconfig/TestConfigForDebug\ntest_string: null\n"

        with open(os.path.join(self.planned_run_dir, 'test_config_for_debug.yaml'), 'w') as file:
            file.write(data)

        with self.assertRaises(expected_exception=Exception):
            self.sc.run(debug=True)


if __name__ == '__main__':
    unittest.main()
