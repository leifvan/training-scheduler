import json
from time import sleep, time
from typing import Dict, Type, Callable, TypeVar, Any, Optional
from yamlable import YamlAble
from .directory_adapters import DirectoryAdapter, ConfigState

CT = TypeVar('CT', bound=YamlAble)


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
                 timeout: Optional[int] = None):
        """
        Creates a new SchedulingClient with the given directory_adapter. It will poll the planned
        directory at most every ``min_polling_interval`` seconds.
        :param directory_adapter: A subclass of DirectoryAdapter.
        :param min_polling_interval: Minimum number of seconds between polling attempts.
        """

        self.directory = directory_adapter
        self.min_polling_interval = min_polling_interval
        self.timeout = timeout
        self.config_consumers: Dict[Type, Callable[[Any], Any]] = dict()

    def register_config(self,
                        config_class: Type,
                        consumer_fn: Callable[[Any], Any]):
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

    def run(self):
        """
        Starts the execution loop of this instance. It will run until the script is aborted with
        an interrupt or an unexpected exception is thrown.
        """

        time_of_last_nonempty_poll = 0

        while True:
            # poll directory for new config files
            files = self.directory.poll()

            time_of_last_poll = time()

            if len(files) > 0:
                time_of_last_nonempty_poll = time_of_last_poll

                # check if there are actually executable configurations
                for file in files:

                    # read config
                    config = self.directory.get_config(file)

                    print("loaded config:", config)

                    # check if there is a consumer for this config
                    if config and type(config) in self.config_consumers:
                        # move config to active folder
                        self.directory.change_state(file, ConfigState.active)

                        # run consumer
                        # TODO maybe the consumers should be run in a separate process to protect from memory leaks
                        try:
                            result = self.config_consumers[type(config)](config)
                        except Exception as e:
                            print("Failed run because of", type(e), e)

                        try:
                            if result is not None:
                                self.directory.write_output(file, json.dumps(result))
                        except Exception as e:
                            print("Failed to write output because of", type(e), e)

                        # complete execution
                        self.directory.change_state(file, ConfigState.completed)
                    else:
                        print("can't do anything with", file)
            else:
                print("no consumable config found")

            # check if we should abort
            if self.timeout and time() - time_of_last_nonempty_poll > self.timeout:
                print("Timeout!")
                return

            # check if we should poll again
            time_delta = self.min_polling_interval - (time() - time_of_last_poll)
            if time_delta > 0:
                print("waiting", time_delta, "secs")
                sleep(time_delta)
