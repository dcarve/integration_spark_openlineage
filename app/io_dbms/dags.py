from io_dbms import DefaultIo


class Dags(DefaultIo):
    def __init__(self):
        last_id_neo4j = self.get_last_id_neo4j(label='Dag')
        self.df = self.get_sql_data(table_name='openlineage.dags', last_id=last_id_neo4j)
        self.label_dict = {"dag": "Dag"}

        self.df = self.df.drop(['owner', 'tribe', 'vertical'], axis=1)

    def convert_to_cypher(self):
        if self.df.shape[0] > 0:
            properties = list(self.df.columns)
            cypher_commands = self.merge_node(self.df, "Dag", properties)

            self.send_logger('Dags.convert_to_cypher')

            return '\n WITH 1 as dummy \n'.join(cypher_commands)
