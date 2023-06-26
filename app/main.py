import asyncio
import logging
import warnings

import uvicorn
from config import docs_api, types
from config.custom_logging import add_logging_level
from config.env_var import create_db_envs
from config.security_service import SecurityService
from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from tasks.connections import Connections
from tasks.read_stream_kafka import ReadStream

create_db_envs()
SecurityService.create_first_user()

warnings.filterwarnings("ignore", category=UserWarning)

add_logging_level('PROCESS_INFO', 70)
logging.basicConfig(level=70)
logger = logging.getLogger('main')

app = FastAPI(
    title=docs_api.title,
    description=docs_api.description,
    version=docs_api.version,
    contact=docs_api.contact,
    openapi_tags=docs_api.tags_metadata,
)

security = HTTPBasic()
read_stream = ReadStream()
connections = Connections()
security_service = SecurityService()


@app.on_event("shutdown")
async def shutdown_event():
    asyncio.create_task(read_stream.stop())


@app.on_event("startup")
async def startup_event():
    asyncio.create_task(read_stream.get_configs())


@app.get("/kafka/stop", tags=["kafka stop"])
async def stop_read_stream(credentials: HTTPBasicCredentials = Depends(security)):
    security_service.verify(credentials)
    asyncio.create_task(read_stream.stop())
    return {"message": "Kafka read stream stopped"}


@app.get("/kafka/restart", tags=["kafka start"])
async def restart_read_stream(credentials: HTTPBasicCredentials = Depends(security)):
    security_service.verify(credentials)
    if not read_stream.running:
        asyncio.create_task(read_stream.start())
        return {"message": "Kafka read stream strated/restarted"}
    else:
        return {"message": "Kafka read stream already running"}


@app.get("/neo4j/on", tags=["Neo4j on"])
async def neo4j_on(credentials: HTTPBasicCredentials = Depends(security)):
    security_service.verify(credentials)
    if not read_stream.neo4j_load:
        asyncio.create_task(read_stream.turn_on_neo4j_load())
        return {"message": "Neo4j load restart"}
    else:
        return {"message": "Neo4j load already on"}


@app.get("/neo4j/off", tags=["Neo4j off"])
async def neo4j_off(credentials: HTTPBasicCredentials = Depends(security)):
    security_service.verify(credentials)
    asyncio.create_task(read_stream.turn_off_neo4j_load())
    return {"message": "Neo4j load stopped"}


@app.post("/change_conn_config", tags=["Change cons configs"])
async def change_conn_configs(
    con_config: types.ConnConfig, credentials: HTTPBasicCredentials = Depends(security)
):
    security_service.verify(credentials)

    res = types.check_input_cons(con_config)

    if res:
        task = asyncio.create_task(read_stream.change_configs(con_config))
        if not task.done():
            await task

        if not read_stream.change_conn_res:
            return {"message": "configuracoes alteradas"}
        else:
            raise HTTPException(status_code=400, detail=read_stream.change_conn_res)
    else:
        raise HTTPException(status_code=400)


@app.get("/test_cons_configured", tags=["Test Conections Configured"])
async def test_all_cons(credentials: HTTPBasicCredentials = Depends(security)):
    security_service.verify(credentials)

    task1 = asyncio.create_task(connections.kafka())
    task2 = asyncio.create_task(connections.postgres())
    task3 = asyncio.create_task(connections.neo4j())

    if not task1.done():
        await task1
    if not task2.done():
        await task2
    if not task3.done():
        await task3

    res = {}
    res["kafka_conn_health"] = connections.kafka_conn
    res["neo4j_conn_health"] = connections.neo4j_conn
    res["postgres_conn_health"] = connections.postgres_conn

    return res


@app.post("/test_custom_connection", tags=["Test Custom Conections"])
async def kakfa_test_custom_connection(
    con_config: types.ConnConfig, credentials: HTTPBasicCredentials = Depends(security)
):
    security_service.verify(credentials)

    res = types.check_input_cons(con_config)

    if res:
        if con_config.type_conn == 'kafka':
            task = asyncio.create_task(connections.kafka(con_config=con_config))
            if not task.done():
                await task
            return {"kafka_conn_health": connections.kafka_conn}

        elif con_config.type_conn == 'postgres':
            task = asyncio.create_task(connections.postgres(con_config=con_config))
            if not task.done():
                await task
            return {"postgres_conn_health": connections.postgres_conn}

        elif con_config.type_conn == 'neo4j':
            task = asyncio.create_task(connections.neo4j(con_config=con_config))
            if not task.done():
                await task
            return {"neo4j_conn_health": connections.neo4j_conn}

    else:
        raise HTTPException(status_code=400)


@app.post("/add_new_user", tags=["Add User"])
async def add_new_user(
    userpass: types.User, credentials: HTTPBasicCredentials = Depends(security)
):
    security_service.verify(credentials)

    res = types.check_input_user(userpass)

    if res:
        task = asyncio.create_task(
            security_service.create_new_user(userpass=userpass, user_request=credentials.username)
        )

        if not task.done():
            await task

        if not security_service.user_unauthorized:
            return {"add new user": userpass.user}
        else:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Incorrect authentification",
                headers={"WWW-Authenticate": "Basic"},
            )
    else:
        raise HTTPException(status_code=400)


if __name__ == "__main__":
    uvicorn.run(app)
