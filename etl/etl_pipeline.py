from queue import Queue
from typing import List

from etl.src import ProcessStep
from etl.pipeline_component import PipelineComponent


class Pipeline:
    def __init__(self, components: List[PipelineComponent], *init_args) -> None:
        self.eventloop = Queue()
        for com in components:
            self.eventloop.put(com)
        self.args = init_args

    def add_component(self, component: PipelineComponent) -> None:
        self.eventloop.put(component)

    def run_pipeline(self) -> None:
        while not self.eventloop.empty():
            p_component: PipelineComponent = self.eventloop.get()
            comp_class = p_component.process_step
            comp_args = p_component.process_args
            breakpoint()

            if comp_args is None:
                component_instance = comp_class(*self.args)
            else:
                component_instance = comp_class(*self.args, *comp_args)

            component_instance.run()
            self.args = component_instance.result()           
