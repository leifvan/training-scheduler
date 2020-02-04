from typing import Any, Iterable, Type, Tuple, Dict
from yamlable import YamlCodec


class ConfigCodec(YamlCodec):
    """A YamlCodec that registers all custom config classes as yamlable. See the yamlable documentation for details."""
    types_to_yaml_tags: Dict[Type, str] = dict()
    yaml_tags_to_types: Dict[str, Type] = dict()

    @classmethod
    def register_type(cls, typ, tag):
        """
        Register a class `typ` as yamlable with the given `tag`. PyYaml will then look for tags in the yaml
        containing the tag `!trainingconfig/[tag]` and parse it into the given `typ`. Instead of registering via
        this function, you can use the `trainingconfig` decorator.
        :param typ: The class that the yaml will be parsed into.
        :param tag: The tag that will be associated with the given class.
        """
        cls.types_to_yaml_tags[typ] = tag
        cls.yaml_tags_to_types[tag] = typ

    @classmethod
    def get_yaml_prefix(cls):
        """
        Returns the tag prefix `!trainingconfig/`.
        :return: The prefix for all training config tags.
        """
        return "!trainingconfig/"  # This is our root yaml tag

    # ----

    @classmethod
    def get_known_types(cls) -> Iterable[Type[Any]]:
        """
        :return: the list of types that we know how to encode
        """
        return cls.types_to_yaml_tags.keys()

    @classmethod
    def is_yaml_tag_supported(cls, yaml_tag_suffix: str) -> bool:
        """
        :param yaml_tag_suffix: The tag to check.
        :return: True if the given yaml tag suffix is supported
        """
        return yaml_tag_suffix in cls.yaml_tags_to_types.keys()

    # ----

    @classmethod
    def from_yaml_dict(cls, yaml_tag_suffix: str, dct, **kwargs):
        """
        Create an object corresponding to the given tag, from the decoded dict.
        :param yaml_tag_suffix: The given tag.
        :param dct: The dictionary to populate the associated class instance with.
        :return: An instance of the class associated with the given tag, containing data from `dct`.
        """
        typ = cls.yaml_tags_to_types[yaml_tag_suffix]
        return typ(**dct)

    @classmethod
    def to_yaml_dict(cls, obj) -> Tuple[str, Any]:
        """
        Encode the given object and also return the tag that it should have
        :param obj: The object to encode.
        :return: A tuple `(tag, dct)` containing the `tag` associated with the type of the given object and a dictionary
        `dct` that can be parsed into a yaml.
        """
        return cls.types_to_yaml_tags[type(obj)], vars(obj)


ConfigCodec.register_with_pyyaml()


def trainingconfig(cls: type):
    """
    A decorator that registers the decorated class in yamlable, so that PyYaml automatically parses it into an instance
    of the decorated class. The corresponding yaml tag will be `!trainingconfig/[classname]`.
    """
    ConfigCodec.register_type(cls, cls.__name__)
    return cls
