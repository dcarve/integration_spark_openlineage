from io_dbms import DefaultIo


class LineageFields(DefaultIo):
    def __init__(self):
        last_id_neo4j = self.get_last_id_neo4j(label='LineageField')
        self.df = self.get_sql_data(table_name='openlineage.lineage_fields', last_id=last_id_neo4j)
        self.label_dict = {"lineage_field": "LineageField"}

    def convert_to_cypher(self):
        if self.df.shape[0] > 0:
            properties = list(self.df.columns)
            cypher_commands = self.merge_node(self.df, "LineageField", properties)

            self.send_logger('LineageFields.convert_to_cypher')

            return '\n WITH 1 as dummy \n'.join(cypher_commands)

    def convert_to_cypher_relations_run(self):
        if self.df.shape[0] > 0:
            where_items = {"lineage_field.run_id": "run.id"}

            relationship = {
                "labels": {"lineage_field": "LineageField", "run": "Run"},
                "r_name": "OCCURS_IN",
                "r_att": "",
            }

            cypher_commands = self.merge_relationship(
                self.df, self.label_dict, where_items, relationship
            )

            self.send_logger('LineageFields.convert_to_cypher_relations_run')

            return cypher_commands

    def convert_to_cypher_relations_field_input(self):
        if self.df.shape[0] > 0:
            where_items = {"lineage_field.field_input": "field.id"}

            relationship = {
                "labels": {"field": "Field", "lineage_field": "LineageField"},
                "r_name": "FIELD_INPUT",
                "r_att": "",
            }
            cypher_commands = self.merge_relationship(
                self.df, self.label_dict, where_items, relationship
            )

            self.send_logger('LineageFields.convert_to_cypher_relations_field_input')

            return cypher_commands

    def convert_to_cypher_relations_field_output(self):
        if self.df.shape[0] > 0:
            where_items = {"lineage_field.field_output": "field.id"}

            relationship = {
                "labels": {"lineage_field": "LineageField", "field": "Field"},
                "r_name": "FIELD_OUTPUT",
                "r_att": "",
            }

            cypher_commands = self.merge_relationship(
                self.df, self.label_dict, where_items, relationship
            )

            self.send_logger('LineageFields.convert_to_cypher_relations_field_output')

            return cypher_commands
