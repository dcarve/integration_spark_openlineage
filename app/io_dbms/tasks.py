from io_dbms import DefaultIo


class Tasks(DefaultIo):
    def __init__(self):
        last_id_neo4j = self.get_last_id_neo4j(label="Task")
        self.df = self.get_sql_data(table_name='openlineage.tasks', last_id=last_id_neo4j)
        self.label_dict = {"task": "Task"}

    def convert_to_cypher(self):
        if self.df.shape[0] > 0:
            properties = list(self.df.columns)
            cypher_commands = self.merge_node(self.df, "Task", properties)

            self.send_logger('Tasks.convert_to_cypher')

            return '\n WITH 1 as dummy \n'.join(cypher_commands)

    def convert_to_cypher_relations(self):
        if self.df.shape[0] > 0:
            where_items = {"task.dag_id": "dag.id"}

            relationship = {
                "labels": {"dag": "Dag", "task": "Task"},
                "r_name": "CONTAINS",
                "r_att": "",
            }

            cypher_commands = self.merge_relationship(
                self.df, self.label_dict, where_items, relationship
            )

            self.send_logger('Tasks.convert_to_cypher_relations')

            return cypher_commands
