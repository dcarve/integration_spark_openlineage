description = """
Projeto que le os logs do Openlineage e envia para postgres e neo4j 
"""


title = "App"
version = "0.0.1"
contact = {
    "name": "Daniel Carvalho",
    "url": "https://github.com/dcarve/integration_spark_openlineage",
    # "email": "",
}

tags_metadata = [
    {
        "name": "kafka start",
        "description": "Inicia o processo de etl stream dos logs do openlineage pelo kafka ",
    },
    {
        "name": "kafka stop",
        "description": "Estado Padrão, Interrompe a leitura do kafka, mas aguarda se alguma msg estiver sendo lida no momento",
    },
    {
        "name": "Change cons configs",
        "description": "Muda configurações das conections do bancos de dados e kafka",
    },
    {
        "name": "Neo4j on",
        "description": "Estado Padrão, Liga a conversão do postgres para o neo4j",
    },
    {
        "name": "Neo4j off",
        "description": "Desliga a conversão do postgres para o neo4j",
    },
    {
        "name": "Test Conections Configured",
        "description": "Testa as conexões com os bancos de dados e kafka",
    },
    {
        "name": "Test Custom Conections",
        "description": "Testa outras conexões para postgres, neo4j e kafka,  usar antes de trocar as configurações",
    },
    {
        "name": "Add User",
        "description": "Add novos usuários na api, somente admin tem acesso",
    },
]
