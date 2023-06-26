// relações entre datasets

MATCH (d1:Dataset)<-[:DATASET_OUTPUT]-(r:Run)<-[:DATASET_INPUT]-(d2:Dataset)
RETURN d1, d2, apoc.create.vRelationship(d2,'LINEAGE_DATASET',{count:count(r)},d1) as rel

// relações entre datasets e dags
MATCH (d:Dataset)<-[:DATASET_OUTPUT]-(r:Run)
WITH r, d
MATCH 
    (dag:Dag)-[:CONTAINS]->(t:Task)<-[:EXECUTED_BY]-(r:Run)
RETURN 
    d,
    dag,
    apoc.create.vRelationship(dag,'output',{count:count(r)},d) as rel

UNION

MATCH (r:Run)<-[:DATASET_INPUT]-(d:Dataset)
WITH r, d
MATCH 
    (dag:Dag)-[:CONTAINS]->(t:Task)<-[:EXECUTED_BY]-(r:Run)
RETURN 
    d,
    dag,
    apoc.create.vRelationship(d,'input',{count:count(r)},dag) as rel
