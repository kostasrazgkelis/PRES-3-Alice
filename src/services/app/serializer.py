from dataclasses import dataclass


@dataclass
class StartTransformationSerializer:
    noise: int
    matching_field: str
    file: str
    columns: []
    name: str
