from typing import Any, Iterable, Type, Tuple, Dict
from yamlable import YamlCodec


class ConfigCodec(YamlCodec):
    types_to_yaml_tags: Dict[Type, str] = dict()
    yaml_tags_to_types: Dict[str, Type] = dict()

    @classmethod
    def register_type(cls, typ, tag):
        cls.types_to_yaml_tags[typ] = tag
        cls.yaml_tags_to_types[tag] = typ
        print("got my new type", typ, tag)

    @classmethod
    def get_yaml_prefix(cls):
        return "!trainingconfig/"  # This is our root yaml tag

    # ----

    @classmethod
    def get_known_types(cls) -> Iterable[Type[Any]]:
        # return the list of types that we know how to encode
        return cls.types_to_yaml_tags.keys()

    @classmethod
    def is_yaml_tag_supported(cls, yaml_tag_suffix: str) -> bool:
        # return True if the given yaml tag suffix is supported
        return yaml_tag_suffix in cls.yaml_tags_to_types.keys()

    # ----

    @classmethod
    def from_yaml_dict(cls, yaml_tag_suffix: str, dct, **kwargs):
        # Create an object corresponding to the given tag, from the decoded dict
        typ = cls.yaml_tags_to_types[yaml_tag_suffix]
        return typ(**dct)

    @classmethod
    def to_yaml_dict(cls, obj) -> Tuple[str, Any]:
        # Encode the given object and also return the tag that it should have
        return cls.types_to_yaml_tags[type(obj)], vars(obj)


ConfigCodec.register_with_pyyaml()


def trainingconfig(cls: type):
    ConfigCodec.register_type(cls, cls.__name__)
    return cls
