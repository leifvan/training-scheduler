import os
from abc import ABC, abstractmethod
from typing import List, Union, Hashable, Generic, TypeVar, Type, Dict, Any
import yaml
from enum import Enum

ConfigState = Enum("ConfigState", "planned active completed")
ConfigType = Any
_allowed_state_changes = ((ConfigState.planned, ConfigState.active),
                          (ConfigState.active, ConfigState.completed))


class DirectoryAdapter(ABC):
    """
    Abstract base class for all directory adapters. Every directory adapter must implement
    polling of new planned configurations, moving them into the active / completed state and
    provide means to add output data to the completed configurations.
    """

    def __init__(self):
        self.identifier_states: Dict[str, ConfigState] = dict()

    def _add_identifier(self, identifier: str, state: ConfigState) -> None:
        """
        Adds ``identifier`` to the internal bookkeeping for identifiers. It will ensure that
        - every identifier is unique
        - the state changes of the identifiers are valid.

        :param identifier: The identifier to add.
        """
        if identifier not in self.identifier_states:
            self.identifier_states[identifier] = state
        else:
            raise Exception(f"Tried to register identifier '{identifier}' that is already present.")

    def change_state(self, identifier: str, next_state: ConfigState,
                     validate_change: bool = True) -> None:
        """
        Changes the state of the config with the given ``identifier`` to ``next_state``.

        :param identifier: The identifier of the config.
        :param next_state: The next state. Must be ``ConfigState.active`` or
        ``ConfigState.completed``.
        :param validate_change: If ``True`` (default), the method will check if the state change
        is an allowed state change, e.g. to prevent an accidental change from completed to planned.
        """
        old_state = self.identifier_states[identifier]

        if validate_change and (old_state, next_state) not in _allowed_state_changes:
            raise ValueError(f"{old_state} -> {next_state} is not a valid state change.")

        self._move_to_state(identifier, self.identifier_states[identifier], next_state)
        self.identifier_states[identifier] = next_state

    @abstractmethod
    def _move_to_state(self, identifier: str, old_state: ConfigState, new_state: ConfigState):
        """
        Method called by the abstract class to execute a state change. It has already been
        validated.
        """
        pass

    @abstractmethod
    def poll_directory(self, state: ConfigState) -> List[str]:
        """
        Check the directory associated with the given ``state`` for valid configs and return their
        unique identifier.
        :param state: The state from which valid configs shall be returned.
        :return: A list of identifiers of configs found.
        """
        pass

    def poll(self) -> List[str]:
        """
        Check the planned directory for valid configs and return their unique identifier.
        :return: A list of identifiers of planned configs found.
        """
        return self.poll_directory(ConfigState.planned)

    @abstractmethod
    def get_config(self, identifier: str) -> ConfigType:
        """
        Get a planned config by its unique identifier. The identifiers can be retrieved with
        ``poll``.
        :param identifier: The unqiue identifier to get the config from.
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
    """
    A DirectoryAdapter that manages configs in separate directories in the local file system.
    """

    def __init__(self, base_dir: Union[str, os.PathLike]):
        """
        Create an adapter that creates several subdirectories in the given ``base_dir``.
        :param base_dir: Path of the root directory used for configs.
        """
        super(LocalDirectoryAdapter, self).__init__()
        self.base_dir = base_dir

        # create folders
        self.directories = dict()
        for state in ConfigState:
            self.directories[state] = os.path.join(self.base_dir, state.name)
            os.makedirs(self.directories[state], exist_ok=True)

    def poll_directory(self, state: ConfigState) -> List[str]:
        with os.scandir(self.directories[state]) as it:
            for de in it:
                if de.path.endswith('.yaml') and de.is_file():
                    identifier = os.path.basename(de.path)
                    if identifier not in self.identifier_states:
                        self._add_identifier(identifier, state)
        return [i for i, s in self.identifier_states.items() if s == state]

    def get_config(self, identifier: str):
        with open(os.path.join(self.directories[ConfigState.planned], identifier)) as file:
            try:
                config = yaml.safe_load(file)
                return config
            except TypeError as e:
                # TODO move prints to a more controllable place, so user can change it
                print("There is an issue with the config", identifier)
                print(e)

    def _move_to_state(self, identifier: str, old_state: ConfigState, new_state: ConfigState):
        _move_to_dir(os.path.join(self.directories[old_state], identifier),
                     self.directories[new_state])

    def write_output(self, identifier: str, output: str) -> None:
        with open(os.path.join(self.directories[ConfigState.completed],
                               identifier + ".out"), 'a') as file:
            file.write(output)
