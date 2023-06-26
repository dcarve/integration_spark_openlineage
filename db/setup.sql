--
-- PostgreSQL database dump
--
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS openlineage;

CREATE TABLE IF NOT EXISTS openlineage.dags (
	"id" SERIAL NOT NULL,
	"name" VARCHAR NOT NULL,
	"owner" VARCHAR,
	"tribe" VARCHAR,
	"vertical" VARCHAR,
    "airflow_env" VARCHAR NOT NULL,
	"create_at"  TIMESTAMP NOT NULL,
	"update_at"  TIMESTAMP NOT NULL
) WITH (
  OIDS=FALSE
)
;

CREATE TABLE IF NOT EXISTS openlineage.tasks (
	"id" SERIAL NOT NULL,
	"name" VARCHAR NOT NULL,
	"dag_id" INT NOT NULL,
	"create_at"  TIMESTAMP NOT NULL,
	"update_at"  TIMESTAMP NOT NULL
) WITH (
  OIDS=FALSE
)
;

CREATE TABLE IF NOT EXISTS openlineage.datasets (
	"id" SERIAL NOT NULL,
	"location"  VARCHAR NOT NULL,
	"uri"  VARCHAR NOT NULL,
	"name"  VARCHAR NOT NULL,
	"namespace"  VARCHAR NOT NULL,
	"create_at"  TIMESTAMP NOT NULL,
	"update_at"  TIMESTAMP NOT NULL
) WITH (
  OIDS=FALSE
)
;

CREATE TABLE IF NOT EXISTS openlineage.schemas (
	"id" SERIAL NOT NULL,
	"schema_json" TEXT NOT NULL,
	"dataset_id" INT NOT NULL,
	"create_at"  TIMESTAMP NOT NULL,
	"update_at"  TIMESTAMP NOT NULL
) WITH (
  OIDS=FALSE
)
;


CREATE TABLE IF NOT EXISTS openlineage.runs (
	"id" SERIAL NOT NULL,
	"spark_logical_plan" TEXT,
	"task_id" INT NOT NULL,
	"event_time"  TIMESTAMP,
	"run_uuid"  VARCHAR,
	"action_type"   VARCHAR,
	"input_dataset_id" INT,
	"output_dataset_id" INT,
	"input_schema_id" INT,
	"output_schema_id" INT,
	"create_at"  TIMESTAMP NOT NULL,
	"update_at"  TIMESTAMP NOT NULL
) WITH (
  OIDS=FALSE
)
;


CREATE TABLE IF NOT EXISTS openlineage.fields (
	"id" SERIAL NOT NULL,
	"schema_id" INT NOT NULL, 
	"name"  VARCHAR NOT NULL,
	"type"  VARCHAR NOT NULL,
	"create_at"  TIMESTAMP NOT NULL,
	"update_at"  TIMESTAMP NOT NULL
) WITH (
  OIDS=FALSE
)
;

CREATE TABLE IF NOT EXISTS openlineage.lineage_fields (
	"id" SERIAL NOT NULL,
	"run_id" INT NOT NULL,
	"field_input" INT NOT NULL, 
	"field_output" INT NOT NULL, 
	"create_at"  TIMESTAMP NOT NULL,
	"update_at"  TIMESTAMP NOT NULL
) WITH (
  OIDS=FALSE
)
;



ALTER TABLE openlineage.dags ADD CONSTRAINT "dag_id_pk0" PRIMARY KEY ("id");
ALTER TABLE openlineage.tasks ADD CONSTRAINT "task_id_pk0" PRIMARY KEY ("id");
ALTER TABLE openlineage.datasets ADD CONSTRAINT "dataset_id_pk0" PRIMARY KEY ("id");
ALTER TABLE openlineage.schemas ADD CONSTRAINT "schema_id_pk0" PRIMARY KEY ("id");
ALTER TABLE openlineage.runs ADD CONSTRAINT "runs_id_pk0" PRIMARY KEY ("id");
ALTER TABLE openlineage.fields ADD CONSTRAINT "fields_id_pk0" PRIMARY KEY ("id");
ALTER TABLE openlineage.lineage_fields ADD CONSTRAINT "lineage_fields_id_pk0" PRIMARY KEY ("id");


ALTER TABLE openlineage.tasks ADD CONSTRAINT "dag_id_fk0" FOREIGN KEY ("dag_id") REFERENCES openlineage.dags("id");

ALTER TABLE openlineage.schemas ADD CONSTRAINT "dataset_id_fk0" FOREIGN KEY ("dataset_id") REFERENCES openlineage.datasets("id");


ALTER TABLE openlineage.runs ADD CONSTRAINT "task_id_fk0" FOREIGN KEY ("task_id") REFERENCES openlineage.tasks("id");
ALTER TABLE openlineage.runs ADD CONSTRAINT "dataset_id_fk1" FOREIGN KEY ("input_dataset_id") REFERENCES openlineage.datasets("id");
ALTER TABLE openlineage.runs ADD CONSTRAINT "dataset_id_fk2" FOREIGN KEY ("output_dataset_id") REFERENCES openlineage.datasets("id");
ALTER TABLE openlineage.runs ADD CONSTRAINT "schema_id_fk0" FOREIGN KEY ("input_schema_id") REFERENCES openlineage.schemas("id");
ALTER TABLE openlineage.runs ADD CONSTRAINT "schema_id_fk1" FOREIGN KEY ("output_schema_id") REFERENCES openlineage.schemas("id");

ALTER TABLE openlineage.fields ADD CONSTRAINT "schema_id_fk2" FOREIGN KEY ("schema_id") REFERENCES openlineage.schemas("id");

ALTER TABLE openlineage.lineage_fields ADD CONSTRAINT "fields_id_fk1" FOREIGN KEY ("field_input") REFERENCES openlineage.fields("id");
ALTER TABLE openlineage.lineage_fields ADD CONSTRAINT "fields_id_fk2" FOREIGN KEY ("field_output") REFERENCES openlineage.fields("id");
ALTER TABLE openlineage.lineage_fields ADD CONSTRAINT "runs_id_fk0" FOREIGN KEY ("run_id") REFERENCES openlineage.runs("id");




CREATE TABLE IF NOT EXISTS staging.example(
   "date"    VARCHAR(10) NOT NULL
  ,"team1"   VARCHAR(12) NOT NULL
  ,"team2"   VARCHAR(12) NOT NULL
  ,"spi1"    VARCHAR(5) NOT NULL
  ,"spi2"    VARCHAR(5) NOT NULL
  ,"prob1"   VARCHAR(6) NOT NULL
  ,"prob2"   VARCHAR(6) NOT NULL
  ,"probtie" VARCHAR(6) NOT NULL
  ,"score1"  VARCHAR(1) NOT NULL
  ,"score2"  VARCHAR(1) NOT NULL
);
INSERT INTO staging.example("date","team1","team2","spi1","spi2","prob1","prob2","probtie","score1","score2") VALUES ('2022-11-20','Qatar','Ecuador','51.0','72.74','0.2369','0.5045','0.2586','0','2');
INSERT INTO staging.example("date","team1","team2","spi1","spi2","prob1","prob2","probtie","score1","score2") VALUES ('2022-11-21','England','Iran','85.96','62.17','0.6274','0.1187','0.2539','6','2');
INSERT INTO staging.example("date","team1","team2","spi1","spi2","prob1","prob2","probtie","score1","score2") VALUES ('2022-11-21','Senegal','Netherlands','73.84','86.01','0.2235','0.5053','0.2712','0','2');
INSERT INTO staging.example("date","team1","team2","spi1","spi2","prob1","prob2","probtie","score1","score2") VALUES ('2022-11-21','USA','Wales','74.83','65.58','0.4489','0.2591','292','1','1');
INSERT INTO staging.example("date","team1","team2","spi1","spi2","prob1","prob2","probtie","score1","score2") VALUES ('2022-11-22','Argentina','Saudi Arabia','87.21','56.87','0.7228','0.0807','0.1966','1','2');
INSERT INTO staging.example("date","team1","team2","spi1","spi2","prob1","prob2","probtie","score1","score2") VALUES ('2022-11-22','Denmark','Tunisia','80.02','65.85','0.5001','0.2054','0.2945','0','0');
INSERT INTO staging.example("date","team1","team2","spi1","spi2","prob1","prob2","probtie","score1","score2") VALUES ('2022-11-22','Mexico','Poland','74.3','68.28','0.4238','0.2802','296','0','0');
INSERT INTO staging.example("date","team1","team2","spi1","spi2","prob1","prob2","probtie","score1","score2") VALUES ('2022-11-22','France','Australia','87.71','60.83','0.6921','0.1009','207','4','1');
INSERT INTO staging.example("date","team1","team2","spi1","spi2","prob1","prob2","probtie","score1","score2") VALUES ('2022-11-23','Morocco','Croatia','75.62','78.84','0.3176','0.3898','0.2926','0','0');
INSERT INTO staging.example("date","team1","team2","spi1","spi2","prob1","prob2","probtie","score1","score2") VALUES ('2022-11-23','Germany','Japan','88.77','71.44','0.6041','174','0.2219','1','2');
INSERT INTO staging.example("date","team1","team2","spi1","spi2","prob1","prob2","probtie","score1","score2") VALUES ('2022-11-23','Spain','Costa Rica','89.51','55.46','0.7623','0.0595','0.1781','7','0');
INSERT INTO staging.example("date","team1","team2","spi1","spi2","prob1","prob2","probtie","score1","score2") VALUES ('2022-11-23','Belgium','Canada','82.49','71.59','0.4888','0.2462','265','1','0');
INSERT INTO staging.example("date","team1","team2","spi1","spi2","prob1","prob2","probtie","score1","score2") VALUES ('2022-11-24','Switzerland','Cameroon','77.65','64.16','0.4991','0.2186','0.2823','1','0');
INSERT INTO staging.example("date","team1","team2","spi1","spi2","prob1","prob2","probtie","score1","score2") VALUES ('2022-11-24','Uruguay','South Korea','80.9','66.12','0.5256','0.1906','0.2838','0','0');
INSERT INTO staging.example("date","team1","team2","spi1","spi2","prob1","prob2","probtie","score1","score2") VALUES ('2022-11-24','Portugal','Ghana','87.77','58.63','0.6974','0.0912','0.2114','3','2');
INSERT INTO staging.example("date","team1","team2","spi1","spi2","prob1","prob2","probtie","score1","score2") VALUES ('2022-11-24','Brazil','Serbia','93.55','75.84','0.6787','0.1124','0.2089','2','0');
INSERT INTO staging.example("date","team1","team2","spi1","spi2","prob1","prob2","probtie","score1","score2") VALUES ('2022-11-25','Wales','Iran','67.64','63.33','0.3875','0.2969','0.3156','0','2');
INSERT INTO staging.example("date","team1","team2","spi1","spi2","prob1","prob2","probtie","score1","score2") VALUES ('2022-11-25','Qatar','Senegal','48.16','73.23','0.2098','0.5228','0.2674','1','3');
INSERT INTO staging.example("date","team1","team2","spi1","spi2","prob1","prob2","probtie","score1","score2") VALUES ('2022-11-25','Netherlands','Ecuador','86.07','74.37','0.4949','0.2248','0.2803','1','1');
INSERT INTO staging.example("date","team1","team2","spi1","spi2","prob1","prob2","probtie","score1","score2") VALUES ('2022-11-25','England','USA','86.26','72.63','0.5436','0.1965','0.2598','0','0');
INSERT INTO staging.example("date","team1","team2","spi1","spi2","prob1","prob2","probtie","score1","score2") VALUES ('2022-11-26','Tunisia','Australia','66.48','58.65','0.3883','0.2894','0.3223','0','1');
INSERT INTO staging.example("date","team1","team2","spi1","spi2","prob1","prob2","probtie","score1","score2") VALUES ('2022-11-26','Poland','Saudi Arabia','68.14','59.01','0.4061','0.2911','0.3029','2','0');
INSERT INTO staging.example("date","team1","team2","spi1","spi2","prob1","prob2","probtie","score1","score2") VALUES ('2022-11-26','France','Denmark','89.25','78.87','0.5064','0.2192','0.2744','2','1');
INSERT INTO staging.example("date","team1","team2","spi1","spi2","prob1","prob2","probtie","score1","score2") VALUES ('2022-11-26','Argentina','Mexico','86.09','73.48','0.5079','0.1993','0.2928','2','0');
INSERT INTO staging.example("date","team1","team2","spi1","spi2","prob1","prob2","probtie","score1","score2") VALUES ('2022-11-27','Japan','Costa Rica','71.98','51.99','0.5955','0.1498','0.2547','0','1');
INSERT INTO staging.example("date","team1","team2","spi1","spi2","prob1","prob2","probtie","score1","score2") VALUES ('2022-11-27','Belgium','Morocco','80.54','74.67','407','0.3057','0.2873','0','2');
INSERT INTO staging.example("date","team1","team2","spi1","spi2","prob1","prob2","probtie","score1","score2") VALUES ('2022-11-27','Croatia','Canada','78.46','73.84','0.4069','0.2893','0.3037','4','1');
INSERT INTO staging.example("date","team1","team2","spi1","spi2","prob1","prob2","probtie","score1","score2") VALUES ('2022-11-27','Spain','Germany','91.22','88.73','0.4619','0.2781','0.26','1','1');
INSERT INTO staging.example("date","team1","team2","spi1","spi2","prob1","prob2","probtie","score1","score2") VALUES ('2022-11-28','Cameroon','Serbia','63.62','74.55','0.2444','0.4575','0.2981','3','3');
INSERT INTO staging.example("date","team1","team2","spi1","spi2","prob1","prob2","probtie","score1","score2") VALUES ('2022-11-28','South Korea','Ghana','66.44','60.03','443','0.2348','0.3222','2','3');
INSERT INTO staging.example("date","team1","team2","spi1","spi2","prob1","prob2","probtie","score1","score2") VALUES ('2022-11-28','Brazil','Switzerland','93.66','77.52','0.6427','0.1202','0.2372','1','0');
INSERT INTO staging.example("date","team1","team2","spi1","spi2","prob1","prob2","probtie","score1","score2") VALUES ('2022-11-28','Portugal','Uruguay','87.3','79.7','0.4392','0.2628','298','2','0');
INSERT INTO staging.example("date","team1","team2","spi1","spi2","prob1","prob2","probtie","score1","score2") VALUES ('2022-11-29','Netherlands','Qatar','84.38','48.46','0.7768','0.0696','0.1536','2','0');
INSERT INTO staging.example("date","team1","team2","spi1","spi2","prob1","prob2","probtie","score1","score2") VALUES ('2022-11-29','Ecuador','Senegal','75.82','73.0','0.3668','0.2961','0.3371','1','2');
INSERT INTO staging.example("date","team1","team2","spi1","spi2","prob1","prob2","probtie","score1","score2") VALUES ('2022-11-29','Iran','USA','65.77','72.55','0.3082','0.3764','0.3154','0','1');
INSERT INTO staging.example("date","team1","team2","spi1","spi2","prob1","prob2","probtie","score1","score2") VALUES ('2022-11-29','Wales','England','65.17','85.41','0.1437','0.6','0.2564','0','3');
INSERT INTO staging.example("date","team1","team2","spi1","spi2","prob1","prob2","probtie","score1","score2") VALUES ('2022-11-30','Tunisia','France','65.92','90.15','0.1116','651','0.2374','1','0');
INSERT INTO staging.example("date","team1","team2","spi1","spi2","prob1","prob2","probtie","score1","score2") VALUES ('2022-11-30','Australia','Denmark','58.8','77.68','0.1985','522','0.2795','1','0');
INSERT INTO staging.example("date","team1","team2","spi1","spi2","prob1","prob2","probtie","score1","score2") VALUES ('2022-11-30','Poland','Argentina','68.95','86.03','0.1547','0.5784','0.2669','0','2');
INSERT INTO staging.example("date","team1","team2","spi1","spi2","prob1","prob2","probtie","score1","score2") VALUES ('2022-11-30','Saudi Arabia','Mexico','58.41','72.44','0.2307','0.4627','0.3066','1','2');
INSERT INTO staging.example("date","team1","team2","spi1","spi2","prob1","prob2","probtie","score1","score2") VALUES ('2022-12-01','Canada','Morocco','71.52','74.99','0.2898','0.3956','0.3147','1','2');
INSERT INTO staging.example("date","team1","team2","spi1","spi2","prob1","prob2","probtie","score1","score2") VALUES ('2022-12-01','Croatia','Belgium','80.87','79.44','0.3881','0.3253','0.2866','0','0');
INSERT INTO staging.example("date","team1","team2","spi1","spi2","prob1","prob2","probtie","score1","score2") VALUES ('2022-12-01','Costa Rica','Germany','52.9','88.86','0.0512','0.8045','0.1442','2','4');
INSERT INTO staging.example("date","team1","team2","spi1","spi2","prob1","prob2","probtie","score1","score2") VALUES ('2022-12-01','Japan','Spain','70.43','90.62','0.1391','0.6135','0.2474','2','1');
INSERT INTO staging.example("date","team1","team2","spi1","spi2","prob1","prob2","probtie","score1","score2") VALUES ('2022-12-02','Ghana','Uruguay','60.5','79.28','0.1564','0.5669','0.2766','0','2');
INSERT INTO staging.example("date","team1","team2","spi1","spi2","prob1","prob2","probtie","score1","score2") VALUES ('2022-12-02','South Korea','Portugal','66.93','87.55','0.1666','0.5866','0.2468','2','1');
INSERT INTO staging.example("date","team1","team2","spi1","spi2","prob1","prob2","probtie","score1","score2") VALUES ('2022-12-02','Serbia','Switzerland','74.72','77.0','0.3126','0.3965','0.2909','2','3');
INSERT INTO staging.example("date","team1","team2","spi1","spi2","prob1","prob2","probtie","score1","score2") VALUES ('2022-12-02','Cameroon','Brazil','64.48','93.48','0.0363','0.8112','0.1524','1','0');
INSERT INTO staging.example("date","team1","team2","spi1","spi2","prob1","prob2","probtie","score1","score2") VALUES ('2022-12-03','Netherlands','USA','83.97','73.07','0.6551','0.3449','0.0','3','1');
INSERT INTO staging.example("date","team1","team2","spi1","spi2","prob1","prob2","probtie","score1","score2") VALUES ('2022-12-03','Argentina','Australia','87.98','59.35','0.8261','0.1739','0.0','2','1');
INSERT INTO staging.example("date","team1","team2","spi1","spi2","prob1","prob2","probtie","score1","score2") VALUES ('2022-12-04','France','Poland','88.57','65.77','0.8133','0.1867','0.0','3','1');
INSERT INTO staging.example("date","team1","team2","spi1","spi2","prob1","prob2","probtie","score1","score2") VALUES ('2022-12-04','England','Senegal','86.97','75.47','0.6814','0.3186','0.0','3','0');
INSERT INTO staging.example("date","team1","team2","spi1","spi2","prob1","prob2","probtie","score1","score2") VALUES ('2022-12-05','Japan','Croatia','73.02','79.14','454','546','0.0','1','1');
INSERT INTO staging.example("date","team1","team2","spi1","spi2","prob1","prob2","probtie","score1","score2") VALUES ('2022-12-05','Brazil','South Korea','92.9','69.4','0.8261','0.1739','0.0','4','1');
INSERT INTO staging.example("date","team1","team2","spi1","spi2","prob1","prob2","probtie","score1","score2") VALUES ('2022-12-06','Morocco','Spain','74.42','89.2','0.2711','0.7289','0.0','0','0');
INSERT INTO staging.example("date","team1","team2","spi1","spi2","prob1","prob2","probtie","score1","score2") VALUES ('2022-12-06','Portugal','Switzerland','85.8','78.51','0.6158','0.3842','0.0','6','1');
INSERT INTO staging.example("date","team1","team2","spi1","spi2","prob1","prob2","probtie","score1","score2") VALUES ('2022-12-09','Croatia','Brazil','78.99','93.47','0.2264','0.7736','0.0','1','1');
INSERT INTO staging.example("date","team1","team2","spi1","spi2","prob1","prob2","probtie","score1","score2") VALUES ('2022-12-09','Netherlands','Argentina','84.19','87.32','0.4238','0.5762','0.0','2','2');
INSERT INTO staging.example("date","team1","team2","spi1","spi2","prob1","prob2","probtie","score1","score2") VALUES ('2022-12-10','Morocco','Portugal','74.45','87.92','0.3185','0.6815','0.0','1','0');
INSERT INTO staging.example("date","team1","team2","spi1","spi2","prob1","prob2","probtie","score1","score2") VALUES ('2022-12-10','England','France','87.59','87.53','516','484','0.0','1','2');
INSERT INTO staging.example("date","team1","team2","spi1","spi2","prob1","prob2","probtie","score1","score2") VALUES ('2022-12-13','Argentina','Croatia','87.46','79.37','0.6426','0.3574','0.0','3','0');
INSERT INTO staging.example("date","team1","team2","spi1","spi2","prob1","prob2","probtie","score1","score2") VALUES ('2022-12-14','France','Morocco','87.72','75.13','0.6646','0.3354','0.0','2','0');
INSERT INTO staging.example("date","team1","team2","spi1","spi2","prob1","prob2","probtie","score1","score2") VALUES ('2022-12-17','Croatia','Morocco','77.65','73.92','0.5325','0.4675','0.0','2','1');
INSERT INTO staging.example("date","team1","team2","spi1","spi2","prob1","prob2","probtie","score1","score2") VALUES ('2022-12-18','Argentina','France','88.86','88.41','0.5321','0.4679','0.0','3','3');
