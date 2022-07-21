from typing import List, Tuple

from etl.src import PipelineComponent 


# def extract(config: dict, dbx: dropbox.Dropbox) -> Tuple[dict, Table]:
#     failed: bool = False

#     ext = Extract(
#         config["BOC"]["url"],
#         dbx,
#         config["BOC"]["startDate"],
#         config["BOC"]["endDate"]
#     )

#     # extract exchange rate data
#     status, boc_data = ext.fetch_data()
#     if status != 200:
#         failed = True

#     ext.save_raw_data(boc_data, failed=failed)

#     if failed:
#         print("Extraction part failed. Aborting pipeline run.")
#         sys.exit()

#     # extract expenses
#     ext.download_expenses()
#     expenses_table = ext.extract_expenses()

#     return boc_data, expenses_table


# def transform(boc_data: dict, expenses_table: Table) -> Table:

#     transformer = Transform(boc_data)
#     exchange_rates_table = transformer.create_table()

#     joined_table = transformer.join_tables(
#         expenses_table,
#         exchange_rates_table
#     )

#     return transformer.add_cad_field(joined_table)


# def load(joined_table: Table, db_conn: Connection):
#     loader = Load(joined_table)
#     loader.load_to_db(db_conn)


# def run_pipeline(config: dict, 
#                  dbx: dropbox.Dropbox, 
#                  db_connection: Connection) -> None:

#     boc_data, expenses_table = extract(config, dbx)

#     joined_table = transform(boc_data, expenses_table)

#     load(joined_table, db_connection)

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

            
