import sqlparse


class DefaultIo:
    @staticmethod
    def convert_pandas_to_mock_sql(df, columns):
        df = df[columns].drop_duplicates()

        columns_name = list(df.columns)
        firts_row = df[0:1]

        def parse_first_row(row):
            cols_string = list()
            for cols in columns_name:
                data = row[cols]
                if isinstance(data, str):
                    data = data.replace("'", "''")
                cols_string.append(f"'{data}' as {cols}")

            return "SELECT " + ', '.join(cols_string)

        firts_row = firts_row.apply(lambda row: parse_first_row(row), axis=1).to_list()

        if df.shape[0] > 1:
            outer_rows = df[1:]

            def parse_outer_rows(row):
                cols_string = list()
                for cols in columns_name:
                    data = row[cols]
                    if isinstance(data, str):
                        data = data.replace("'", "''")
                    cols_string.append(f"'{data}'")

                return "UNION (SELECT " + ", ".join(cols_string) + ')'

            outer_rows = outer_rows.apply(lambda row: parse_outer_rows(row), axis=1).to_list()

        else:
            outer_rows = []

        return '\n'.join(firts_row + outer_rows)

    @staticmethod
    def insert_statement(select, where, columns, table_to_insert):
        query = f"""
            WITH insert_statement AS (
                {select}
            WHERE
                ({where})
            )
            INSERT INTO {table_to_insert} ({', '.join(columns)})
            SELECT {', '.join(columns)} FROM insert_statement

            """
        return query

    @staticmethod
    def update_statement(select, where, columns, table_to_update):
        for idx, col in enumerate(columns):
            if idx == 0:
                where_update = f"{table_to_update}.{col}=subquery.{col}"
            else:
                where_update = where_update + f"\n and {table_to_update}.{col}=subquery.{col}"

        query = f"""
            WITH update_statement AS (
                {select}
                WHERE
                    not ({where})
                )
            UPDATE {table_to_update}
            SET
                update_at=current_timestamp
            FROM (SELECT {', '.join(columns)} FROM update_statement) AS subquery
            WHERE
                {where_update}
            """
        return query

    @staticmethod
    def prettier_sql(query):
        return sqlparse.format(query, reindent=True, keyword_case='upper')
