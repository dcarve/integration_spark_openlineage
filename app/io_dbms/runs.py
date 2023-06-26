from io_dbms import DefaultIo


class Runs(DefaultIo):
    def __init__(self):
        last_id_neo4j = self.get_last_id_neo4j(label='Run')
        self.df = self.get_sql_data(table_name='openlineage.runs', last_id=last_id_neo4j)
        self.label_dict = {"run": "Run"}

        self.df['event_time'] = self.df['event_time'].astype(str)

    def convert_to_cypher(self):
        if self.df.shape[0] > 0:
            properties = list(self.df.columns)
            cypher_commands = self.merge_node(self.df, "Run", properties)

            self.send_logger('Runs.convert_to_cypher')

            return '\n WITH 1 as dummy \n'.join(cypher_commands)

    def convert_to_cypher_relations_task(self):
        if self.df.shape[0] > 0:
            where_items = {"run.task_id": "task.id"}

            relationship = {
                "labels": {"run": "Run", "task": "Task"},
                "r_name": "EXECUTED_BY",
                "r_att": "",
            }

            cypher_commands = self.merge_relationship(
                self.df, self.label_dict, where_items, relationship
            )

            self.send_logger('Runs.convert_to_cypher_relations_task')

            return cypher_commands

    def convert_to_cypher_relations_dataset_input(self):
        if self.df.shape[0] > 0:
            where_items = {"run.input_dataset_id": "dataset.id"}

            relationship = {
                "labels": {"dataset": "Dataset", "run": "Run"},
                "r_name": "DATASET_INPUT",
                "r_att": "",
            }

            cypher_commands = self.merge_relationship(
                self.df, self.label_dict, where_items, relationship
            )

            self.send_logger('Runs.convert_to_cypher_relations_dataset_input')

            return cypher_commands

    def convert_to_cypher_relations_dataset_output(self):
        if self.df.shape[0] > 0:
            where_items = {"run.output_dataset_id": "dataset.id"}

            relationship = {
                "labels": {"run": "Run", "dataset": "Dataset"},
                "r_name": "DATASET_OUTPUT",
                "r_att": "",
            }

            cypher_commands = self.merge_relationship(
                self.df, self.label_dict, where_items, relationship
            )

            self.send_logger('Runs.convert_to_cypher_relations_dataset_output')

            return cypher_commands

    def convert_to_cypher_relations_schema_input(self):
        if self.df.shape[0] > 0:
            where_items = {"run.input_schema_id": "schema.id"}

            relationship = {
                "labels": {"schema": "Schema", "run": "Run"},
                "r_name": "SCHEMA_INPUT",
                "r_att": "",
            }

            cypher_commands = self.merge_relationship(
                self.df, self.label_dict, where_items, relationship
            )

            self.send_logger('Runs.convert_to_cypher_relations_schema_input')

            return cypher_commands

    def convert_to_cypher_relations_schema_output(self):
        if self.df.shape[0] > 0:
            where_items = {"run.output_schema_id": "schema.id"}

            relationship = {
                "labels": {"run": "Run", "schema": "Schema"},
                "r_name": "SCHEMA_OUTPUT",
                "r_att": "",
            }

            cypher_commands = self.merge_relationship(
                self.df, self.label_dict, where_items, relationship
            )

            self.send_logger('Runs.convert_to_cypher_relations_schema_output')

            return cypher_commands
