# script that observes a given directory
# execute a template using a yaml as configuration
# templates are client specific

"""
template_name: a name
template_config:
    config1: value1
    config2: value2
"""
from directory_adapters import DirectoryAdapter, LocalDirectoryAdapter, ConfigBaseClass, ConfigContainer
from time import sleep, time
from typing import Dict, List, Any, Type, Callable, TypeVar, NewType
import subprocess
import yaml
from yamlable import YamlAble, yaml_info
from dataclasses import dataclass

T = TypeVar('T', bound=ConfigBaseClass)


class SchedulingClient:
    def __init__(self,
                 directory_adapter: DirectoryAdapter,
                 min_polling_interval: float=10):

        self.directory = directory_adapter
        self.min_polling_interval = min_polling_interval
        self.config_consumers: Dict[Type[ConfigBaseClass], Callable[[ConfigBaseClass], None]] = dict()

    def register_config(self,
                        config_class: Type[T],
                        consumer_fn: Callable[[T], None]):

        if config_class in self.config_consumers:
            raise Exception(f"There already is a consumer for {config_class}.")

        self.config_consumers[config_class] = consumer_fn

    def run(self):

        while True:
            # poll directory for new config files
            files = self.directory.poll_planned_directory()

            last_time = time()

            if len(files) > 0:
                # check if there are actually executable configurations
                for file in files:

                    # read config
                    config = self.directory.get_config(file)

                    print("loaded config:", config)

                    # check if there is a consumer for this config
                    if config and type(config) in self.config_consumers:
                        # move config to active folder
                        self.directory.move_to_active_directory(file)

                        # run consumer
                        self.config_consumers[type(config)](config)

                        # complete execution
                        self.directory.move_to_completed_directory(file)
                    else:
                        print("can't do anything with", file)
            else:
                print("nothing of interest found")

            # check if we should poll again
            time_delta = self.min_polling_interval - (time() - last_time)
            if time_delta > 0:
                print("waiting", time_delta, "secs")
                sleep(time_delta)


if __name__ == "__main__":
    sc = SchedulingClient(directory_adapter=LocalDirectoryAdapter("test"))

    def consumer(config: ConfigContainer) -> None:
        print("run")
        sleep(3)
        print("end")

    sc.register_config(config_class=ConfigContainer, consumer_fn=consumer)
    sc.run()