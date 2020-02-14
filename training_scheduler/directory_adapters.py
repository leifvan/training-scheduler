import os
from abc import ABC, abstractmethod
from typing import List, Union, Hashable, Generic, TypeVar, Type, Dict
import yaml
from enum import Enum

ConfigState = Enum("ConfigState", "planned active completed")


class DirectoryAdapter(ABC):
    """
    Abstract base class for all directory adapters. Every directory adapter must implement
    polling of new planned configurations, moving them into the active / completed state and
    provide means to add output data to the completed configurations.
    """

    def __init__(self):
        self.identifier_states: Dict[str, ConfigState] = dict()

    def _add_identifier(self, identifier: str) -> None:
        """
        Adds ``identifier`` to the internal bookkeeping for identifiers. It will ensure that
        - every identifier is unique
        - the state changes of the identifiers are valid.

        :param identifier: The identifier to add.
        """
        if identifier not in self.identifier_states:
            self.identifier_states[identifier] = ConfigState.planned

    def change_state(self, identifier: str, next_state: ConfigState) -> None:
        """
        Changes the state of the config with the given ``identifier`` to ``next_state``.

        :param identifier: The identifier of the config.
        :param next_state: The next state. Must be ``ConfigState.active`` or ``ConfigState.completed``.
        """
        # check that to_state is the next state
        assert next_state == ConfigState(self.identifier_states[identifier].value + 1)
        self._move_to_state(identifier, self.identifier_states[identifier], next_state)
        self.identifier_states[identifier] = next_state

    @abstractmethod
    def _move_to_state(self, identifier: str, old_state: ConfigState, new_state: ConfigState):
        """


        :param identifier:
        :param old_state:
        :param new_state:
        :return:
        """
        pass

    @abstractmethod
    def poll(self) -> List[str]:
        """
        Check the planned directory for valid configs and return their unique identifier.
        :return: A list of identifiers of all configs found.
        """
        pass

    @abstractmethod
    def get_config(self, identifier: str) -> Union[Type, None]:
        """
        Get the config by its unique identifier. The identifiers can be retrieved with ``poll_planned_directory``.
        :param identifier: The unqiue identifier too get the config from.
        :return: An instance of the class associated with the yaml tag of that config.
        """
        pass

    @abstractmethod
    def write_output(self, identifier: str, output: str) -> None:
        """
        Writes ``output`` in the output file corresponding to ``identifier``.

        :param identifier: The unique identifier to write output for.
        :param output: A ``str`` that will be appended to the output file.
        :return:
        """
        pass


def _move_to_dir(file: Union[os.PathLike, str], dir: Union[os.PathLike, str]) -> None:
    """
    Move ``file`` into directory ``dir``.

    :param file: The path of the file.
    :param dir: The path of the target directory.
    """
    basename = os.path.basename(file)
    assert os.path.isdir(dir)
    os.rename(file, os.path.join(dir, basename))


class LocalDirectoryAdapter(DirectoryAdapter):
    def __init__(self, base_dir: Union[str, os.PathLike]):
        super(LocalDirectoryAdapter, self).__init__()
        self.base_dir = base_dir
        self.planned_dir = os.path.join(self.base_dir, ConfigState.planned.name)
        self.active_dir = os.path.join(self.base_dir, ConfigState.active.name)
        self.completed_dir = os.path.join(self.base_dir, ConfigState.completed.name)
        os.makedirs(self.planned_dir, exist_ok=True)
        os.makedirs(self.active_dir, exist_ok=True)
        os.makedirs(self.completed_dir, exist_ok=True)

    def poll(self):
        with os.scandir(self.planned_dir) as it:
            new_identifiers = []
            for de in it:
                if de.path.endswith('.yaml') and de.is_file():
                    identifier = os.path.basename(de.path)
                    self._add_identifier(identifier)
                    new_identifiers.append(identifier)
            return new_identifiers

    def get_config(self, identifier: str):
        with open(os.path.join(self.planned_dir, identifier)) as file:
            try:
                config = yaml.safe_load(file)
                return config
            except TypeError as e:
                print("There is an issue with the config", identifier)
                print(e)

    def _move_to_state(self, identifier: str, old_state: ConfigState, new_state: ConfigState):
        if old_state == ConfigState.planned:
            _move_to_dir(os.path.join(self.planned_dir, identifier), self.active_dir)
        elif old_state == ConfigState.active:
            _move_to_dir(os.path.join(self.active_dir, identifier), self.completed_dir)

    def write_output(self, identifier: str, output: str) -> None:
        with open(os.path.join(self.completed_dir, identifier + ".out"), 'a') as file:
            file.write(output)
