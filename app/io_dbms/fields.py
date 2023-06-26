from io_dbms import DefaultIo


class Fields(DefaultIo):
    def __init__(self):
        last_id_neo4j = self.get_last_id_neo4j(label='Field')
        self.df = self.get_sql_data(table_name='openlineage.fields', last_id=last_id_neo4j)
        self.label_dict = {"field": "Field"}

    def convert_to_cypher(self):
        if self.df.shape[0] > 0:
            properties = list(self.df.columns)
            cypher_commands = self.merge_node(self.df, "Field", properties)

            self.send_logger('Fields.convert_to_cypher')

            return '\n WITH 1 as dummy \n'.join(cypher_commands)

    def convert_to_cypher_relations(self):
        if self.df.shape[0] > 0:
            where_items = {"field.schema_id": "schema.id"}

            relationship = {
                "labels": {"schema": "Schema", "field": "Field"},
                "r_name": "CONTAINS",
                "r_att": "",
            }

            cypher_commands = self.merge_relationship(
                self.df, self.label_dict, where_items, relationship
            )

            self.send_logger('fields.convert_to_cypher_relations')

            return cypher_commands
