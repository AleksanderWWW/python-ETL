from collections import deque
from typing import List

from etl.pipeline_component import PipelineComponent


class Pipeline:
    def __init__(self, components: List[PipelineComponent], *init_args) -> None:
        self.eventloop = deque()
        for com in components:
            self.eventloop.append(com)
        self.args = init_args

    def add_component_right(self, component: PipelineComponent) -> None:
        if not isinstance(component, PipelineComponent):
            raise TypeError(f"Expected component to be of type PipelineComponent. Got {type(component)} instead")
        self.eventloop.append(component)

    def add_component_left(self, component: PipelineComponent) -> None:
        if not isinstance(component, PipelineComponent):
            raise TypeError(f"Expected component to be of type PipelineComponent. Got {type(component)} instead")
        self.eventloop.appendleft(component)

    def run_pipeline(self) -> None:
        while self.eventloop:
            breakpoint()
            p_component: PipelineComponent = self.eventloop.popleft()
            comp_class = p_component.process_step
            comp_args = p_component.process_args
            
            if comp_args is None:
                component_instance = comp_class(*self.args)
            else:
                component_instance = comp_class(*self.args, *comp_args)

            component_instance.run()
            self.args = component_instance.result           
