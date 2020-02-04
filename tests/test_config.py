import unittest
from typing import List
from training_scheduler.config import trainingconfig, ConfigCodec
from dataclasses import dataclass
import yaml


class TestConfig(unittest.TestCase):
    def test_if_trainingconfig_can_be_loaded_and_dumped(self):
        @trainingconfig
        @dataclass
        class _SomeConfig:
            some_string: str
            some_int: int
            some_list: List[int]

        obj_str = "!trainingconfig/_SomeConfig\nsome_string: abc\nsome_int: 3\nsome_list: [1,2,3]"
        obj = yaml.safe_load(obj_str)
        self.assertIs(type(obj), _SomeConfig)
        str_again = yaml.dump(obj)
        obj_again = yaml.safe_load(str_again)
        self.assertEqual(obj, obj_again)

if __name__ == '__main__':
    unittest.main()
