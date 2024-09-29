from abc import ABC, abstractmethod
from dataclasses import dataclass, asdict
import json


@dataclass
class ClickEvent:
    ip: str
    app: str
    device: str
    os: str
    channel: str
    click_time: str
    attributed_time: str
    is_attributed: bool

    def to_json(self):
        return json.dumps(asdict(self), default=str)

    @staticmethod
    def from_json(json_data):
        data = json.loads(json_data)
        return ClickEvent(**data)


class AppException(Exception):
    pass


class Reader(ABC):
    @abstractmethod
    def publish_input(self):
        pass


class SlidingWindow(ABC):
    pass


class Operation(ABC):
    @abstractmethod
    def add_element(self, element: ClickEvent):
        pass

    @abstractmethod
    def remove_element(self, event_time):
        pass