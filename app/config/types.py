from typing import Any, AnyStr, Dict, List, Optional, Union

from pydantic import BaseModel
from pydantic.main import ModelMetaclass

JSONObject = Dict[AnyStr, Any]
JSONArray = List[Any]
JSONStructure = Union[JSONArray, JSONObject]


class AllOptional(ModelMetaclass):  # pragma: no cover
    def __new__(cls, name, bases, namespaces, **kwargs):
        annotations = namespaces.get('__annotations__', {})
        for base in bases:
            annotations.update(base.__annotations__)
        for field in annotations:
            if not field.startswith('__'):
                annotations[field] = Optional[annotations[field]]
        namespaces['__annotations__'] = annotations
        return super().__new__(cls, name, bases, namespaces, **kwargs)


class ChangeConnConfigBase(BaseModel):  # pragma: no cover
    type_conn: str
    host: str
    port: str
    usr: str
    pwd: str
    db: str
    topic: str


class User(BaseModel):  # pragma: no cover
    user: str
    pwd: str
    role: str


class ConnConfig(ChangeConnConfigBase, metaclass=AllOptional):  # pragma: no cover
    pass


def check_input_cons(input):
    if (
        ((input.type_conn == 'kafka') and hasattr(input, 'host') and hasattr(input, 'port'))
        or (
            (input.type_conn == 'postgres')
            and hasattr(input, 'host')
            and hasattr(input, 'port')
            and hasattr(input, 'db')
            and hasattr(input, 'usr')
            and hasattr(input, 'pwd')
        )
        or (
            (input.type_conn == 'neo4j')
            and hasattr(input, 'host')
            and hasattr(input, 'port')
            and hasattr(input, 'usr')
            and hasattr(input, 'pwd')
        )
    ):
        return True


def check_input_user(input):
    if (
        hasattr(input, 'user')
        and hasattr(input, 'pwd')
        and hasattr(input, 'role')
        and (input.role in ['admin', 'default'])
    ):
        return True
