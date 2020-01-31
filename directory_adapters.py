import os
from abc import ABC, abstractmethod
from typing import List, Union, Hashable, Generic, TypeVar, Text, Type
import yaml

IdentifierT = TypeVar('IdentifierT', bound=Hashable)


class DirectoryAdapter(ABC, Generic[IdentifierT]):
    """
    Abstract base class for all directory adapters. Every directory adapter must implement
    polling of new planned configurations, moving them into the active / completed state and
    provide means to add output data to the completed configurations.
    """

    @abstractmethod
    def poll_planned_directory(self) -> List[IdentifierT]:
        pass

    @abstractmethod
    def get_config(self, identifier: IdentifierT) -> Union[Type, None]:
        pass

    @abstractmethod
    def get_output_path(self, identifier: IdentifierT) -> Union[os.PathLike, str]:
        pass

    @abstractmethod
    def move_to_active_directory(self, identifier: IdentifierT):
        pass

    @abstractmethod
    def move_to_completed_directory(self, identifier: IdentifierT):
        pass


def move_to_dir(file: Union[os.PathLike, str], dir: Union[os.PathLike, str]):
    basename = os.path.basename(file)
    assert os.path.isdir(dir)
    os.rename(file, os.path.join(dir, basename))


class LocalDirectoryAdapter(DirectoryAdapter[Text]):
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

    def get_config(self, identifier: Text):
        with open(os.path.join(self.planned_dir, identifier)) as file:
            try:
                config = yaml.safe_load(file)
                return config
            except TypeError as e:
                print("There is an issue with the config", identifier)
                print(e)

    def get_output_path(self, identifier: Text):
        if os.path.exists(os.path.join(self.active_dir, identifier)):
            out_name = identifier.rpartition('.')[0]+".out"
            return os.path.join(self.active_dir, out_name)
        raise FileNotFoundError

    def move_to_active_directory(self, identifier: Text):
        move_to_dir(os.path.join(self.planned_dir, identifier), self.active_dir)

    def move_to_completed_directory(self, identifier: Text):
        out_path = self.get_output_path(identifier)
        if os.path.exists(out_path):
            move_to_dir(out_path, self.completed_dir)

        move_to_dir(os.path.join(self.active_dir, identifier), self.completed_dir)
