import asyncio
import datetime as dt
import sqlite3

import bcrypt
import pandas as pd
from fastapi import HTTPException, status
from fastapi.security import HTTPBasicCredentials
from google.cloud import secretmanager

PROJECT_ID = "maga-bigdata"


class SecurityService:
    def __init__(self):
        self.user_unauthorized = None

    @staticmethod
    def verify(credentials: HTTPBasicCredentials) -> None:
        password = SecurityService.get_pass(credentials.username)
        if password:
            correct_credentials = bcrypt.checkpw(credentials.password.encode(), password)
        else:
            correct_credentials = False

        if not correct_credentials:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Incorrect authentification",
                headers={"WWW-Authenticate": "Basic"},
            )

    @staticmethod
    def get_pass(username):  # pragma: no cover
        conn = SecurityService.create_conn()

        pwd = pd.read_sql(
            f"""SELECT user, pass, date FROM credencials WHERE user='{username}' ORDER BY date DESC LIMIT 1""",
            conn,
        )
        conn.close()

        if pwd.shape[0] > 0:
            return pwd['pass'].to_list()[0]
        else:
            return ''

    @staticmethod
    def access_secret_version(secret_id, version_id="latest"):
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{PROJECT_ID}/secrets/{secret_id}/versions/{version_id}"
        response = client.access_secret_version(name=name)

        return response.payload.data.decode('UTF-8')

    @staticmethod
    def create_conn():  # pragma: no cover
        path = "config/user_auth.db"
        conn = sqlite3.connect(path)
        return conn

    @staticmethod
    def create_first_user():  # pragma: no cover
        # admin:1234

        conn = SecurityService.create_conn()

        list_tables = pd.read_sql(
            """SELECT name FROM sqlite_master WHERE type='table' and name='credencials';""", conn
        )

        if list_tables.shape[0] == 0:
            cur = conn.cursor()
            cur.execute(
                """CREATE TABLE IF NOT EXISTS credencials (
                    user text,
                    pass text,
                    role text,
                    date text
                )"""
            )
            conn.commit()
            cur.execute(
                f"""INSERT INTO credencials (
                    user,
                    pass,
                    role,
                    date
                )
                VALUES (
                    'admin',
                    '$2a$12$Ir2O8.3CfFCQKiIIGOAODemN7zHbL4o.dOvT3OLojhC5F4ij6w3H2',
                    'admin',
                    '{dt.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')}'
                )"""
            )
            conn.commit()
            conn.close()

    async def create_new_user(self, userpass, user_request):  # pragma: no cover
        conn = SecurityService.create_conn()

        response_role = pd.read_sql(
            f"""SELECT role FROM credencials WHERE user='{user_request}' ORDER BY date DESC LIMIT 1;""",
            conn,
        )['role'].to_list()[0]

        if response_role == 'admin':
            cur = conn.cursor()
            cur.execute(
                f"""INSERT INTO credencials (
                    user,
                    pass,
                    role,
                    date
                )
                VALUES (
                    '{userpass.user}',
                    '{bcrypt.hashpw(userpass.pwd.encode(), bcrypt.gensalt())}',
                    '{userpass.role}',
                    '{dt.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')}'
                )"""
            )
            conn.commit()
            conn.close()

            self.user_unauthorized = None

        else:
            self.user_unauthorized = True

        await asyncio.sleep(3)
