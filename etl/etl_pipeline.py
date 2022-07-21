from typing import List, Tuple

from etl.src import PipelineComponent 


class Pipeline:
    def __init__(self, components: List[Tuple[PipelineComponent, Tuple]], *init_args) -> None:
        self.components = components
        self.args = init_args

    def run_pipeline(self):
        for component in self.components:
            comp_class, comp_args = component

            if comp_args is None:
                component_instance = comp_class(*self.args)
            else:
                component_instance = comp_class(*self.args, *comp_args)

            component_instance.run()
            self.args = component_instance.result()

            
