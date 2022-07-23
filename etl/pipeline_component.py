from dataclasses import dataclass
from typing import Union, Tuple

from etl.src import ProcessStep


@dataclass
class PipelineComponent:
    process_step: ProcessStep
    process_args: Union[Tuple, None]
