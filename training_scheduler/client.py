import json
from time import sleep, time
from typing import Dict, Type, Callable, Any, Optional
from yamlable import YamlAble
from .directory_adapters import DirectoryAdapter, ConfigState, ConfigType
from abc import abstractmethod, ABC

ConsumerCallbackType = Callable[[YamlAble, str], Any]


class SchedulingClientCallback:
    """
    The SchedulingClientCallback provides an interface to react to common events occurring in
    the run process of the SchedulingClient.
    """

    def on_config_loaded(self, identifier: str, config: ConfigType) -> None:
        """
        Fired when a config file was loaded succesfully.
        :param identifier: The identifier of the config.
        :param config: The config object.
        """
        ...

    def on_failed_to_write_result(self, identifier: str, config: ConfigType,
                                  results: Optional[dict], exception: Exception) -> None:
        """
        Fired when an exception occurs while writing the results of the config consumer.
        :param identifier: The identifier of the config.
        :param config: The config object.
        :param results: The output of the registered config consumer.
        :param exception: The exception caught while writing.
        """
        ...

    def on_failed_to_run_config(self, identifier: str, config: ConfigType,
                                exception: Exception) -> None:
        """
        Fired when an exception occurs while running the config consumer for ``config``.
        :param identifier: The identifier of the config.
        :param config: The config object.
        :param exception: The exception caught while running.
        """
        ...

    def on_unregistered_config(self, identifier: str, config: ConfigType) -> None:
        """
        Fired when a config was found that has no registered consumer.
        :param identifier: The identifier of the config.
        :param config: The config object.
        """
        ...

    def on_no_configs_found(self) -> None:
        """
        Fired when no config were found in the last poll.
        """
        ...

    def on_waiting_for_next_poll(self, delta: float) -> None:
        """
        Fired when there is still time between the last polling attempt and the next one and the
        run loop is about to sleep for ``delta`` seconds.
        :param delta: The time in seconds until the next poll will be attempted.
        """
        ...

    def on_timeout(self):
        """
        Fired when the run loop is about to be exited because of the preset timeout.
        """
        ...


class DefaultSchedulingClientCallback(SchedulingClientCallback):
    """
    An exemplary implementation of ``SchedulingClientCallback`` that prints some debug info for each
    event.
    """

    def on_config_loaded(self, identifier: str, config: ConfigType) -> None:
        print("loaded config:", config)

    def on_failed_to_write_result(self, identifier: str, config: ConfigType,
                                  results: Optional[dict], exception: Exception) -> None:
        print("Failed to write results because of", type(exception), exception)

    def on_failed_to_run_config(self, identifier: str, config: ConfigType,
                                exception: Exception) -> None:
        print("Failed run because of", type(exception), exception)

    def on_unregistered_config(self, identifier: str, config: ConfigType) -> None:
        print("can't do anything with", identifier)

    def on_no_configs_found(self) -> None:
        print("no consumable config found")

    def on_waiting_for_next_poll(self, delta: float) -> None:
        print("waiting", delta, "secs")

    def on_timeout(self):
        print("Timeout!")


class SchedulingClient:
    """
    The SchedulingClient is observing a directory for new configurations using a directory adapter.
    The observed directory is expected to have three subfolders: planned_runs, active_runs and
    completed_runs. The client will regularly poll the planned_runs directory to check for new
    configs and execute them if the config has a registered config consumer.
    """

    def __init__(self,
                 directory_adapter: DirectoryAdapter,
                 min_polling_interval: int = 10,
                 timeout: Optional[int] = None,
                 callback: Optional[SchedulingClientCallback] = DefaultSchedulingClientCallback()):
        """
        Creates a new SchedulingClient with the given directory_adapter. It will poll the planned
        directory at most every ``min_polling_interval`` seconds.
        :param directory_adapter: A subclass of DirectoryAdapter.
        :param min_polling_interval: Minimum number of seconds between polling attempts.
        """

        self.directory = directory_adapter
        self.min_polling_interval = min_polling_interval
        self.timeout = timeout
        self.callback = SchedulingClientCallback() if callback is None else callback

        self.config_consumers: Dict[Type, ConsumerCallbackType] = dict()

    def register_config(self,
                        config_class: Type,
                        consumer_fn: ConsumerCallbackType):
        """
        Registers a consumer for a given type of config. The config has to be defined by a config class
        decorated with @config.trainingconfig. The yaml decoder will then look for a config file with
        tag ``!trainingconfig/classname``, where classname is the name of the config class. If a config
        with this tag is found, the given ``consumer_fn`` will be called, passing the config as the
        first parameter. The return value of ``consumer_fn`` will be parsed with ``json.dump`` and saved
        alongside the config file in the ``completed_runs`` directory.
        :param config_class: The class to be consumed by ``consumer_fn``.
        :param consumer_fn: A function that consumes configs of type ``config_class`` and possibly returns
        a json-serializable result object.
        """

        if config_class in self.config_consumers:
            raise Exception(f"There already is a consumer for {config_class}.")

        self.config_consumers[config_class] = consumer_fn

    def _resume_active_configs(self):
        # check for active configs
        active_configs = self.directory.poll_directory(ConfigState.active)

        if len(active_configs) > 0:
            print("There",
                  "is 1 config" if len(active_configs) == 1
                  else f"are {len(active_configs)} configs",
                  "marked as active:")

            for identifier in active_configs:
                print(" -", identifier)
                self.directory.change_state(identifier, ConfigState.planned,
                                            validate_change=False)

            print("It was" if len(active_configs) == 1 else "They were", "moved back into the",
                  "planned directory to be resumed.")

    def run(self, debug=False, resume_active_configs=False) -> None:
        """
        Starts the execution loop of this instance. It will run until the script is aborted with
        an interrupt or an unexpected exception is thrown.
        :param debug: If true, the run loop will re-raise all exceptions occurring during execution
        of consumers (defaults to false).
        :param resume_active_configs: If true, all configs found in the active directory are moved
        back into the planned directory before starting the run loop (defaults to false).
        """

        if resume_active_configs:
            self._resume_active_configs()

        time_of_last_nonempty_poll = 0

        while True:
            # poll directory for new config files
            identifiers = self.directory.poll()

            time_of_last_poll = time()

            if len(identifiers) > 0:
                time_of_last_nonempty_poll = time_of_last_poll

                # check if there are actually executable configurations
                for identifier in identifiers:

                    # read config
                    config = self.directory.get_config(identifier)

                    self.callback.on_config_loaded(identifier, config)

                    # check if there is a consumer for this config
                    if config and type(config) in self.config_consumers:
                        # move config to active folder
                        self.directory.change_state(identifier, ConfigState.active)

                        # run consumer
                        try:
                            result = self.config_consumers[type(config)](config, identifier)
                            try:
                                if result is not None:
                                    self.directory.write_output(identifier, json.dumps(result))

                            except Exception as e:
                                self.callback.on_failed_to_write_result(identifier, config, result, e)
                                if debug: raise e

                        except Exception as e:
                            self.callback.on_failed_to_run_config(identifier, config, e)
                            if debug: raise e

                        # complete execution
                        self.directory.change_state(identifier, ConfigState.completed)
                    else:
                        self.callback.on_unregistered_config(identifier, config)
            else:
                self.callback.on_no_configs_found()

            # check if we should abort
            if self.timeout and time() - time_of_last_nonempty_poll > self.timeout:
                self.callback.on_timeout()
                return

            # check if we should poll again
            time_delta = self.min_polling_interval - (time() - time_of_last_poll)
            if time_delta > 0:
                self.callback.on_waiting_for_next_poll(time_delta)
                sleep(time_delta)
