from typing import List, Union, Dict, Any
import os
import yaml
from yamlable import YamlAble, yaml_info
from dataclasses import dataclass
from abc import ABC, abstractmethod


class ConfigBaseClass:
    pass


@yaml_info(yaml_tag_ns='trainingscheduler.config')
@dataclass
class ConfigContainer(YamlAble, ConfigBaseClass):
    template_name: str
    template_config: Any


class DirectoryAdapter(ABC):

    @abstractmethod
    def poll_planned_directory(self) -> List[str]:
        pass

    @abstractmethod
    def get_config(self, identifier) -> Union[ConfigBaseClass, None]:
        pass

    @abstractmethod
    def get_output_path(self, identifier) -> Union[os.PathLike, str]:
        pass

    @abstractmethod
    def move_to_active_directory(self, identifier):
        pass

    @abstractmethod
    def move_to_completed_directory(self, identifier):
        pass


def move_to_dir(file, dir):
    basename = os.path.basename(file)
    assert os.path.isdir(dir)
    os.rename(file, os.path.join(dir, basename))


class LocalDirectoryAdapter(DirectoryAdapter):
    def __init__(self, base_dir: Union[str, os.PathLike]):
        self.base_dir = base_dir
        self.planned_dir = os.path.join(self.base_dir, 'planned_runs')
        self.active_dir = os.path.join(self.base_dir, 'active_runs')
        self.completed_dir = os.path.join(self.base_dir, 'completed_runs')
        os.makedirs(self.planned_dir, exist_ok=True)
        os.makedirs(self.active_dir, exist_ok=True)
        os.makedirs(self.completed_dir, exist_ok=True)

    def poll_planned_directory(self):
        with os.scandir(self.planned_dir) as it:
            return [os.path.basename(de.path) for de in it if de.path.endswith('.yaml') and de.is_file()]

    def get_config(self, identifier):
        with open(os.path.join(self.planned_dir, identifier)) as file:
            try:
                config = yaml.safe_load(file)
                if isinstance(config, ConfigBaseClass):
                    return config
                print(identifier, "is a config without correct class definition.")
            except TypeError as e:
                print("There is an issue with the config", identifier)
                print(e)

    def get_output_path(self, identifier: str):
        if os.path.exists(os.path.join(self.active_dir, identifier)):
            out_name = identifier.rpartition('.')[0]+".out"
            return os.path.join(self.active_dir, out_name)
        raise FileNotFoundError

    def move_to_active_directory(self, identifier: str):
        move_to_dir(os.path.join(self.planned_dir, identifier), self.active_dir)

    def move_to_completed_directory(self, identifier: str):
        out_path = self.get_output_path(identifier)
        if os.path.exists(out_path):
            move_to_dir(out_path, self.completed_dir)

        move_to_dir(os.path.join(self.active_dir, identifier), self.completed_dir)
