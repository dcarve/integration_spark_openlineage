## Getting started

Subir uma instance local do kafka, postgres e neo4j
```bash
docker compose -f docker-compose-kafka.yml up
docker compose -f docker-docker-compose-postgres-neo4j.yml up
```
Criar um ambiente conda
```bash
conda create -n project python=3.8 -y
conda activate project
make install
```
Rodar aplicação
```bash
make run_app
```
Swagger UI
```sh
http://localhost:8086/docs#/
```

## curl api endpoints

### GET commands

Basic Authentication
Exemplo:
base64('user:pass') = dXNlcjpwYXNz
base64('admin:1234') = dXNlcjpwYXNz


Inicia/reinicia o loop assíncrono do kafka comsumer, necessário após subir a aplicação, pois o padrão é desligado
```bash
curl -i -H "Accept: application/json" -H "Content-Type: application/json" -H "Authorization: Basic dXNlcjpwYXNz" -X GET http://localhost:8086/kafka/restart
```

Interrompe o loop kafka comsumer
```bash
curl -i -H "Accept: application/json" -H "Content-Type: application/json" -H "Authorization: Basic dXNlcjpwYXNz" -X GET http://localhost:8086/kafka/stop
```

Interrompe o processo que envia os dados para o neo4j
```bash
curl -i -H "Accept: application/json" -H "Content-Type: application/json" -H "Authorization: Basic dXNlcjpwYXNz" -X GET http://localhost:8086/neo4j/off
```

Reinicia o processo que envia os dados para o neo4j, o processo pode ser parado sem ter perda de dado, pois o neo4j recebe os dados direto do postgres

```bash
curl -i -H "Accept: application/json" -H "Content-Type: application/json" -H "Authorization: Basic dXNlcjpwYXNz" -X GET http://localhost:8086/neo4j/on
```

Testa se as configurações atuais de conexões do postgres, neo4j e kafka estão ok
```bash
curl -i -H "Accept: application/json" -H "Content-Type: application/json" -H "Authorization: Basic dXNlcjpwYXNz" -X GET http://localhost:8086/test_cons_configured
```
### POST commands

Teste conexões de configurações customizadas, recomendado antes de trocar por novas configurações
```bash
# postgres
curl -X POST http://localhost:8086/test_custom_connection -H 'Content-Type: application/json' -H "Authorization: Basic dXNlcjpwYXNz" -d '{"type_conn": "postgres","host": "localhost","port": "5432","usr": "openlineage","pwd": "openlineage","db": "openlineage"}'

# kafka
curl -X POST http://localhost:8086/test_custom_connection -H 'Content-Type: application/json' -H "Authorization: Basic dXNlcjpwYXNz" -d '{"type_conn": "kafka","host": "localhost","port": "9092", "topic":"nome_topico"}'

# neo4j
curl -X POST http://localhost:8086/test_custom_connection -H 'Content-Type: application/json' -H "Authorization: Basic dXNlcjpwYXNz" -d '{"type_conn": "neo4j","host": "localhost","port": "7687","usr": "neo4j","pwd": "openlineage"}'
```

Trocar configurações de conexões
```bash
# postgres
curl -X POST http://localhost:8086/change_conn_config -H 'Content-Type: application/json' -H "Authorization: Basic dXNlcjpwYXNz" -d '{"type_conn": "postgres","host": "localhost","port": "5432","usr": "openlineage","pwd": "openlineage","db": "openlineage"}'

# kafka
curl -X POST http://localhost:8086/change_conn_config -H 'Content-Type: application/json' -H "Authorization: Basic dXNlcjpwYXNz" -d '{"type_conn": "kafka","host": "localhost","port": "9092"}'

# neo4j
curl -X POST http://localhost:8086/change_conn_config -H 'Content-Type: application/json' -H "Authorization: Basic dXNlcjpwYXNz" -d '{"type_conn": "neo4j","host": "localhost","port": "7687","usr": "neo4j","pwd": "openlineage"}'
```

Adcionar novos usuários
```bash
curl -X POST http://localhost:8086/add_new_user -H 'Content-Type: application/json' -H "Authorization: Basic dXNlcjpwYXNz" -d '{"user": "harley","pwd": "ivy", "role": "default"}'
```

## Desenho do processo
```mermaid

flowchart LR

    subgraph SparkMinecraft
        SparkAction[Spark Action]
        Log[Create OpenLineage log]
        KafkaProducer[Log sent to Kafka Producer]
    end

    subgraph App

        subgraph AsyncLoopReadKafka
            direction TB
    
            KafkaConsumer[Create Kafka Consumer]
            ReadConsumer[Read msgs from consumer]
            ETL[Parse msgs]
            Postgres[Send to Postgres]
            Neo4j[Send to Neo4j]
            CommitMsg[Commit msgs readed]
            KafkaConsumerClose[Close Kafka Consumer]
        
        end

        subgraph AsyncStopKafka
    
            KafkaStop[Stop Kafka Consumer]
        
        end

        subgraph Configs

            ChangeCons[Change connections configs of DBs]
            TestCons[test connections of DBs]
            TestCustom[test custom connections]

        end

    end

    SparkAction --> Log --> KafkaProducer
    KafkaConsumer --> ReadConsumer --> ETL
    ETL --> Postgres --> Neo4j --> CommitMsg
    CommitMsg --> KafkaConsumerClose

    SparkMinecraft --> |KAFKA| App
```

## Modelo relacional

<p align="center" width="100%">
    <img src="docs/source/postgres_diagram.svg">
</p>

## Modelo de grafo

```mermaid

flowchart LR

    dag((Dag))
    task((Task))
    run((Run))
    field((Field))
    lineage_field((Lineage<br>Field))
    schema((Schema))
    dataset((Dataset))

    dag-->|CONTAINS|task
    run-->|EXECUTED_BY|task
    run-->|SCHEMA_OUTPUT|schema
    schema-->|SCHEMA_INPUT|run
    run-->|DATASET_OUTPUT|dataset
    dataset-->|DATASET_INPUT|run
    dataset-->|CONTAINS|schema-->|CONTAINS|field
    lineage_field-->|OCCURS_IN|run
    lineage_field-->|FIELD_OUTPUT|field
    field-->|FIELD_INPUT|lineage_field

```