from typing import Protocol, Callable, Any


class DbObject(Protocol):
    """Abstract interface to a database"""

    def execute(*args, **kwargs):
        """Method to execute an SQL statement on a database"""


DbObjectCreator = Callable[[Any], DbObject]
