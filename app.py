from flask import Flask, request
from dotenv import load_dotenv
import os
import psycopg2
load_dotenv()  # loads variables from .env file into environment

app = Flask(__name__)
url = os.environ.get("DATABASE_URL")  # gets variables from environment
connection = psycopg2.connect(url)

CREATE_STATS_TABLE = "CREATE TABLE IF NOT EXISTS STATS (mcuuid UUID NOT NULL, worldname text, name text, value bigint);"
INSERT_STAT = "INSERT INTO STATS (mcuuid, worldname, name, value) VALUES (%s, %s, %s, %s)"
SEARCH_WORLD_STAT = "SELECT * FROM STATS WHERE mcuuid = (%s) AND worldname = (%s)"
ADD_USER = "INSERT INTO mcuser(uuid, username) VALUES (%s, %s)"

@app.post("/api/addworld")
def add_stat():
    data = request.get_json()
    uuid = data["uuid"]
    worldname = data["worldname"]
    statistics = data["data"]
    statistics = tuple(statistics)
    with connection:
        with connection.cursor() as cursor:
            res = cursor.execute("SELECT name FROM STATS WHERE mcuuid = (%s) AND worldname = (%s)", (uuid, worldname,))
            existing_stats = res.fetchall()
            cursor.execute(CREATE_STATS_TABLE)
            for stat in statistics:
                if stat[0].strip() in existing_stats:
                    # we dont want to add a duplicate stat so we'll update instead
                    cursor.execute("UPDATE TABLE STATS SET value = (%s) WHERE worldname = (%s) AND mcuuid = (%s) AND name = (%s)", (stat[1], uuid, worldname, stat[0]))
                    continue
                cursor.execute(INSERT_STAT, (uuid, worldname, stat[0], stat[1],))
    return {"message": f"Added {len(statistics)} stats under world '{worldname}'."}, 201

@app.get("/api/getdata/<string:uuid>/<string:world>")
def get_all_stats(uuid, world):
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(SEARCH_WORLD_STAT, (uuid, world,))
            result = cursor.fetchall()
            if result is not None:
                for s in result:
                    print(s)
    if result is not None: return {"message": f"{len(result)} result(s).", "result": result}, 201
    else: return {"message": "0 results.", "result": {}}, 201

@app.get("/api/getname/<string:uuid>/<string:world>")
def get_worldname(uuid, world):
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(SEARCH_WORLD_STAT, (uuid, world,))
            result = cursor.fetchall()
            if result is not None:
                for s in result:
                    print(s)
    if result is not None: return {"message": f"{len(result)} result(s).", "result": result}, 201
    else: return {"message": "0 results.", "result": {}}, 201

@app.get("/api/getname/<string:uuid>")
def get_mcuser(uuid, world):
    with connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT * FROM mcuser WHERE uuid = (%s)", (uuid,))
            result = cursor.fetchall()
            if result is not None:
                for s in result:
                    print(s)
    if result is not None: return {"message": f"{len(result)} result(s).", "result": result}, 201
    else: return {"message": "0 results.", "result": {}}, 201

@app.post("/api/adduser")
def add_user():
    data = request.get_json()
    uuid = data["uuid"]
    username = data["username"]
    with connection:
        with connection.cursor() as cursor:
            res = cursor.execute("SELECT * FROM mcuser WHERE uuid = (%s)", (uuid,))
            user = res.fetchall()
            if user is not None:
                # we dont want to add a duplicate stat so we'll update instead
                return {"message": f"User already exists in database."}, 201
            cursor.execute(ADD_USER, (uuid, username,))
    return {"message": f"User {username} was added to database."}, 201