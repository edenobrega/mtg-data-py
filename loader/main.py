import sqlalchemy as sa
import pandas as pd
import numpy as np
import logging as lo
import datetime as dt
import requests
import json
from sys import exit
from os import mkdir, path, getenv, remove
from dotenv import load_dotenv
from time import sleep

# Variables
BULK_NAME: str = None
CONN_STR: str = None
DB_NAME: str = None
LOAD_STRAT: str = None
LOG_LEVEL: int = None
engine: sa.Engine = None
log: lo.Logger = None

#region Helpers
def exit_as_failed(reason: str = None):
    if reason is not None:
        log.critical(reason)
    log.critical("loader exiting as failed")
    raise SystemExit

def get_from_db(sql: str):
    return pd.read_sql(sql, engine)

def request_set_cards(_uri, data):
    # api asks for a 50ms to 100ms wait between requests
    sleep(0.15)
    try:
        req_data = requests.get(_uri).json()
    except Exception as ex:
        log.exception("error while sending request : "+_uri)
        return data

    data = data + req_data["data"]
    if req_data["has_more"]:
        data = request_set_cards(req_data["next_page"], data)
    return data

def create_connection(connection_string: str, database_name: str) -> sa.Engine:
    engine = sa.create_engine(connection_string)
    try:
        log.debug("testing db connection")
        with engine.connect() as conn:
            conn.execute(sa.text("SELECT 1"))
        log.debug("connection success")
        log.debug("checking if tables exist")
        with engine.connect() as conn:
            sql = sa.text("""
                            SET NOCOUNT ON
                          
                            DECLARE @tablenames TABLE([Name] NVARCHAR(25)) 
                            INSERT INTO @tablenames VALUES
                            ('CardPart'),
                            ('Layout'),
                            ('Set'),
                            ('CardType'),
                            ('SetType'),
                            ('CardFace'),
                            ('Card'),
                            ('TypeLine'),
                            ('Rarity')

                            SELECT 1
                            FROM
                            (
                                SELECT COUNT(1) as [Count]
                                FROM [INFORMATION_SCHEMA].[TABLES] 
                                WHERE [TABLE_SCHEMA] = 'MTG' 
                                AND [TABLE_TYPE] = 'BASE TABLE'
                                AND [TABLE_NAME] IN (SELECT [Name] FROM @tablenames)
                                AND [TABLE_CATALOG] = :db_name 
                            ) AS a
                            WHERE a.[Count] = (SELECT COUNT(1) FROM @tablenames)
                        """)
            sql = sql.bindparams(db_name=database_name)
            schema_check = conn.execute(sql).one_or_none()
            if schema_check is None:
                log.critical("tables are missing from the schema")
                exit_as_failed()
            log.debug("tables exist")
    except Exception as ex:
        log.critical("failed to connect to db : %s", ex)
        exit_as_failed()
    return engine
#endregion

# Get data from its configured source
# https://scryfall.com/docs/api/cards
def load_data() -> pd.DataFrame:
    card_frame: pd.DataFrame = None
    sets_frame: pd.DataFrame = None

    if LOAD_STRAT == "DOWNLOAD":
        log.info("downloading bulk data")
        bulk_catalog = "https://api.scryfall.com/bulk-data"
        req: requests.Response = requests.get(bulk_catalog)
        if req.status_code != 200:
            log.critical("bulk data request failed : %s %s", req.status_code, req.reason)
            exit_as_failed()
        catalog = req.json()
        if "data" not in catalog and "default_cards" not in catalog["data"]:
            log.critical("bulk data reading failed, data to get bulk file noth found")
            exit_as_failed()

        bulk_uri = next(obj["download_uri"] for obj in catalog["data"] if obj["type"] == "default_cards")
        bulk_data = requests.get(bulk_uri).json()

        if path.exists(BULK_NAME):
            log.warning("deleteing existing bulk file : "+BULK_NAME)
            remove(BULK_NAME)

        with open(BULK_NAME, 'x', encoding='utf-8') as f:
            json.dump(bulk_data, f, ensure_ascii=False, indent=4)

        # TODO: Can probably just load straight from the variable
        card_frame = pd.read_json(BULK_NAME, orient='records')
        sets_frame = card_frame.loc[:, ["set_name", "set", "set_search_uri", "set_type", "set_id"]].drop_duplicates()
        log.info("finished loading from download")
    elif LOAD_STRAT == "LOCAL":
        log.info("loading from bulk data file")
        if not path.exists(BULK_NAME):
            log.critical("bulk file does not exist")
            exit_as_failed()
        card_frame = pd.read_json(BULK_NAME, orient='records')
        sets_frame = card_frame.loc[:, ["set_name", "set", "set_search_uri", "set_type", "set_id"]].drop_duplicates()
        log.info("finished loading from bulk data file")
    elif LOAD_STRAT == "SETS":
        exit_as_failed("not recommended for now")
        # TODO: need to rename columns to match how its like when it gets loaded from locally
        db_sets = get_from_db("SELECT [shorthand], [icon], [source_id], [release_date] FROM [MTG].[Set]")        
        # load from api
        SETS_API_URI = "https://api.scryfall.com/sets"
        log.info("requesting sets data from %s", SETS_API_URI)

        sets_req: requests.Response = requests.get(SETS_API_URI)
        if sets_req.status_code != 200:
            log.critical("sets data request failed : %s %s", sets_req.status_code, sets_req.reason)
            exit_as_failed() 
        api_sets = sets_req.json()
        if "data" not in api_sets:
            log.critical("no data object in response")
            exit_as_failed() 
        api_frame = pd.DataFrame.from_dict(api_sets["data"])

        # Get all sets that dont exist in the db
        #       add missing to the update list
        sets_frame = api_frame.loc[~api_frame["id"].isin(db_sets["source_id"]), :]
        log.info("%s new sets found", sets_frame.shape[0])

        # Get all card counts from api and compare to count(*) from db
        #       add those that arent equal to update list
        # Check for null values in db_sets and update from api
        db_set_counts = get_from_db("""
                                    SELECT s.shorthand AS [Shorthand], COUNT(card_set_id) AS [db_count]
                                    FROM mtg.Card AS c
                                    JOIN mtg.[Set] AS s ON s.id = c.card_set_id
                                    GROUP BY s.shorthand
                                    """)
        api_counts = api_frame.loc[:, ["code", "card_count", "search_uri"]]
        needs_update = pd.merge(api_counts, db_set_counts, left_on="code", right_on="Shorthand", how="left")
        needs_update = needs_update.loc[needs_update["db_count"] != needs_update["card_count"], :]

        log.info("getting card data from requests")
        raw_card_data = []
        for uri in needs_update.loc[needs_update["card_count"] > 0, :]["search_uri"]:
            cards = request_set_cards(uri, [])
            raw_card_data = raw_card_data + cards

        log.info("finished getting card data from requests")
        card_frame = pd.DataFrame.from_dict(raw_card_data)
        
    return card_frame, sets_frame

# Ensure sure that all required columns are present
def prepare_card_frame(cards: pd.DataFrame, sets_frame: pd.DataFrame) -> pd.DataFrame:
    pass

def save_to_db(cards: pd.DataFrame, sets: pd.DataFrame):
    log.info("Beginning load . . .")
    
    # Set Type
    log.info("checking for new set types")  
    tf_settypes = sets["set_type"].copy().to_frame(name="name").drop_duplicates()
    db_settypes = get_from_db("SELECT [name] FROM [MTG].[SetType]")
    new_settypes = tf_settypes.loc[~tf_settypes["name"].isin(db_settypes["name"]), :]
    if new_settypes.shape[0] > 0:
        new_settypes.to_sql(
            schema="MTG",
            name="SetType",
            con=engine,
            index=False,
            if_exists="append"
        )
        log.info("new set types added")
    else:
        log.info("no new set types found")

    # Sets
    log.info("checking for new sets")
    sets_source_ids = sets.copy()
    db_sets = get_from_db("select [source_id] from mtg.[Set]")
    new_sets: pd.DataFrame = sets_source_ids.loc[~sets_source_ids["set_id"].isin(db_sets["source_id"])]
    if new_sets.shape[0] > 0:
        new_sets = new_sets.rename(columns={
            "set_id":"source_id",
            "set":"shorthand",
            "set_search_uri":"search_uri",
            "set_type":"set_type_id",
            "set_name":"name"
        })
        settype_lookup = get_from_db("select [id], [name] from [MTG].[SetType]").set_index("name")
        settype_lookup["id"] = settype_lookup["id"].astype("str")
        settype_lookup = settype_lookup.to_dict()["id"]
        new_sets["set_type_id"] = new_sets["set_type_id"].map(settype_lookup)
        new_sets["set_type_id"] = new_sets["set_type_id"].astype("int")
        new_sets.to_sql(
            schema="MTG",
            name="Set",
            con=engine,
            index=False,
            if_exists="append"
        )
        log.info("new sets added")
    else:
        log.info("no new sets found")


if __name__ == "__main__":
    load_dotenv()

    BULK_NAME = getenv("TCGCT_BULK_NAME")
    LOG_LEVEL = int(getenv('TCGCT_LOG_LEVEL'))
    LOAD_STRAT = str(getenv("TCGCT_LOAD_STRAT"))
    DB_NAME = getenv("TCGCT_DB_NAME")    
    CONN = getenv("TCGCT_CONNECTION_STRING").replace("##DB_NAME##", DB_NAME)

    if CONN is None:
        exit_as_failed("no connection string defined")

    if DB_NAME is None:
        exit_as_failed("no db name defined")

    if LOAD_STRAT is None or LOAD_STRAT not in ["LOCAL", "DOWNLOAD", "SETS"]:
        exit_as_failed("No LOAD_STRAT defined")

    if LOG_LEVEL is None:
        LOG_LEVEL = 20

    if BULK_NAME is None:
        BULK_NAME = "data/bulk_data.json"

    if not path.isdir('logs'):
        mkdir('logs\\')

    lo.basicConfig(level=lo._levelToName[LOG_LEVEL],
                    filename='logs\\'+str(dt.datetime.today().date())+'.txt',
                    format='%(asctime)s | %(levelname)s | Line:%(lineno)s | %(message)s',
                    filemode='a')
    log = lo.getLogger(__name__)

    for foreignLogger in lo.Logger.manager.loggerDict:
        if foreignLogger not in [__name__]:
            lo.getLogger(foreignLogger).disabled = True

    log.info("loader started started")

    engine = create_connection(CONN, DB_NAME)

    cards, sets = load_data()
    x = prepare_card_frame(cards, sets)
    save_to_db(cards, sets)
