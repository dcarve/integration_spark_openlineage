from io_dbms import DefaultIo


class Datasets(DefaultIo):
    def __init__(self):
        last_id_neo4j = self.get_last_id_neo4j(label='Dataset')
        self.df = self.get_sql_data(table_name='openlineage.datasets', last_id=last_id_neo4j)
        self.label_dict = {"dataset": "Dataset"}

    def convert_to_cypher(self):
        if self.df.shape[0] > 0:
            properties = list(self.df.columns)
            cypher_commands = self.merge_node(self.df, "Dataset", properties)

            self.send_logger('Datasets.convert_to_cypher')

            return '\n WITH 1 as dummy \n'.join(cypher_commands)
