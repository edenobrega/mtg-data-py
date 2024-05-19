import sqlalchemy as sa
import pandas as pd
import numpy as np
import logging as lo
import datetime as dt
import requests
import json
import mtg_transform as mt
from sys import exit
from os import mkdir, path, getenv, remove
from dotenv import load_dotenv
from time import sleep

#region Constants
BULK_NAME: str = None
CONN_STR: str = None
DB_NAME: str = None
LOAD_STRAT: str = None
LOG_LEVEL: int = None
engine: sa.Engine = None
log: lo.Logger = None
#endregion

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
            # TODO: Perhaps move into its own proc?
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

def extract() -> pd.DataFrame:
    card_frame: pd.DataFrame = None
    sets_frame: pd.DataFrame = None
    update_sets_data: pd.DataFrame = None

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
            log.warning("deleting existing bulk file : "+BULK_NAME)
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
    elif LOAD_STRAT == "API":
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

        # Get all sets that have null values
        null_sets = get_from_db("""
                                SELECT source_id, null as [empty]
                                FROM [MTG].[Set]
                                WHERE icon IS NULL OR release_date IS NULL
                                """)

        update_sets_data = api_frame.loc[api_frame["id"].isin(null_sets["source_id"]), ["id", "icon_svg_uri", "released_at"]]


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
        
    return card_frame, sets_frame, update_sets_data

def transform(cards_raw: pd.DataFrame, sets_frame: pd.DataFrame):
    cards: pd.DataFrame = None
    faces: pd.DataFrame = None
    parts: pd.DataFrame = None
    type_lines: pd.DataFrame = None
    types: pd.DataFrame = None
    # TODO: ensure same columns as when comes from api and comes from bulkdata
    sets: pd.DataFrame = sets_frame.copy()
    sets = sets.rename(columns={"id":"set_id"})

    rarities = mt.get_rarities(cards_raw)
    layouts = mt.get_layouts(cards_raw)
    types, type_lines = mt.get_type_line_data(cards_raw)
    faces = mt.get_card_faces(cards_raw)
    parts = mt.get_card_parts(cards_raw)
    cards = mt.get_cards(cards_raw)

    return cards, faces, parts, type_lines, types, rarities, layouts, sets

# TODO: Make bulk inserts faster
#       Potentionally use https://bcp.readthedocs.io/en/latest/
def save_to_db(cards: pd.DataFrame, sets: pd.DataFrame, faces: pd.DataFrame, parts: pd.DataFrame, type_lines: pd.DataFrame, types: pd.DataFrame, rarities: pd.Series, layouts: pd.Series, sets_info: pd.DataFrame) -> None:
    '''Final transformations to match DB and insert into tables '''

    log.info("Beginning load . . .")
    
    #region Update Sets
    if sets_info.shape[0] > 0:
        sets_info = sets_info.rename(columns={
            "id": "source_id",
            "icon_svg_uri": "icon",
            "released_at": "release_date"
        })

        with engine.connect() as conn:
            update_sql = """
                        DECLARE @temp TABLE
                        (
                            source_id NVARCHAR(36), 
                            icon NVARCHAR(300), 
                            release_date DATE
                        )

                        INSERT INTO @temp
                        VALUES 
                        """
            
            value_array = sets_info.values
            for val in value_array:
                update_sql += "('"+val[0]+"', '"+val[1]+"', '"+val[2]+"'),"

            update_sql = update_sql[:-1]

            update_sql +=   """

                            UPDATE s
                            SET s.icon = t.icon, s.release_date = t.release_date
                            FROM [MTG].[Set] as s
                            JOIN @temp AS t ON t.source_id = s.source_id
                            """
            conn.execute(sa.text(update_sql))
            conn.commit()
    #endregion

    #region Set Type
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
    #endregion

    #region Sets
    log.info("checking for new sets")
    sets_source_ids = sets.copy()
    db_sets = get_from_db("select [source_id] from mtg.[Set]")
    new_sets: pd.DataFrame = sets_source_ids.loc[~sets_source_ids["set_id"].isin(db_sets["source_id"])]
    if new_sets.shape[0] > 0:
        new_sets = new_sets.rename(columns={
            "set_id":"source_id",
            "set":"shorthand",
            "code":"shorthand",
            "set_search_uri":"search_uri",
            "set_type":"set_type_id",
            "set_name":"name",
            "icon_svg_uri":"icon",
            "released_at":"release_date"
        })
        settype_lookup = get_from_db("select [id], [name] from [MTG].[SetType]").set_index("name")
        settype_lookup["id"] = settype_lookup["id"].astype("str")
        settype_lookup = settype_lookup.to_dict()["id"]
        new_sets["set_type_id"] = new_sets["set_type_id"].map(settype_lookup)
        new_sets["set_type_id"] = new_sets["set_type_id"].astype("int")
        if LOAD_STRAT == "API":
            new_sets = new_sets.loc[:, ["source_id", "shorthand", "search_uri", "set_type_id", "name", "icon", "release_date"]]
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
    #endregion

    #region Rarity
    log.info("checking for new rarities")
    db_rarities = get_from_db("select [name] from [MTG].[Rarity]")
    new_rarities = rarities.copy().loc[~rarities.isin(db_rarities["name"])]
    if new_rarities.shape[0] > 0:
        new_rarities.name = "name"
        new_rarities.to_sql(
            schema="MTG",
            name="Rarity",
            con=engine,
            index=False,
            if_exists="append"
        )
        log.info("new rarities added")
    else:
        log.info("no new rarities found")
    #endregion

    #region Layout
    log.info("checking for new layouts")
    db_layouts = get_from_db("select [name] from [MTG].[Layout]")
    new_layouts = layouts.copy().loc[~layouts.isin(db_layouts["name"])]
    if new_layouts.shape[0] > 0:
        new_layouts.name = "name"
        new_layouts.to_sql(
            schema="MTG",
            name="Layout",
            con=engine,
            index=False,
            if_exists="append"
        )
        log.info("new layouts added")
    else:
        log.info("no new layouts found")
    #endregion

    #region Card Types
    db_card_types = get_from_db("SELECT [name] FROM [MTG].[CardType]")
    new_card_types: pd.DataFrame = types.loc[~types["type_line"].isin(db_card_types["name"])].copy()
    if new_card_types.shape[0] > 0:
        new_card_types = new_card_types.rename(columns={"type_line":"name"})
        log.info("adding new card types . . .")
        new_card_types.to_sql(
            schema="MTG",
            name="CardType",
            con=engine,
            index=False,
            if_exists="append"
        )
        log.info("new card types added")
    else:
        log.info("no new card types to add")
    #endregion

    #region Card
    log.info("checking for new cards")
    db_card_ids = get_from_db("SELECT [source_id], [id] FROM [MTG].[Card]")
    new_cards: pd.DataFrame = cards.copy().loc[~cards["id"].isin(db_card_ids["source_id"])]
    if new_cards.shape[0] > 0:
        rarity_lookup = get_from_db("select [id], [name] from [MTG].[Rarity]").set_index("name")
        layout_lookup = get_from_db("select [id], [name] from [MTG].[Layout]").set_index("name")
        set_lookup = get_from_db("SELECT [id], [shorthand] FROM [MTG].[Set]").set_index("shorthand")
        
        rarity_lookup["id"] = rarity_lookup["id"].astype("str")
        layout_lookup["id"] = layout_lookup["id"].astype("str")
        set_lookup["id"] = set_lookup["id"].astype("str")

        rarity_lookup = rarity_lookup.to_dict()["id"]
        layout_lookup = layout_lookup.to_dict()["id"]
        set_lookup = set_lookup.to_dict()["id"]

        new_cards["rarity"] = new_cards["rarity"].map(rarity_lookup)
        new_cards["layout"] = new_cards["layout"].map(layout_lookup)
        new_cards["set"] = new_cards["set"].map(set_lookup)

        new_cards["rarity"] = new_cards["rarity"].astype("int")
        new_cards["layout"] = new_cards["layout"].astype("int")
        new_cards["set"] = new_cards["set"].astype("int")

        new_cards["collector_number"] = new_cards["collector_number"].astype("str")

        # TODO: move this into its own reusable function
        new_cards["power"] = np.where(pd.isnull(new_cards["power"]),new_cards["power"],new_cards["power"].astype("str"))
        new_cards["toughness"] = np.where(pd.isnull(new_cards["toughness"]),new_cards["toughness"],new_cards["toughness"].astype("str"))
        new_cards["loyalty"] = np.where(pd.isnull(new_cards["loyalty"]),new_cards["loyalty"],new_cards["loyalty"].astype("str"))

        new_cards.loc[new_cards["mana_cost"] == "", "mana_cost"] = None

        new_cards = new_cards.rename(columns={
            "oracle_text":"text",
            "flavor_text":"flavor",
            "set": "card_set_id",
            "id":"source_id",
            "cmc":"converted_cost",
            "normal":"image",
            "rarity": "rarity_id",
            "layout": "layout_id"
        })

        log.info("adding new cards . . .")
        new_cards.to_sql(
            schema="MTG",
            name="Card",
            con=engine,
            index=False,
            if_exists="append"
        )

        log.info("new cards added")
    else:
        log.info("no new cards found")
    #endregion

    #region Card Face
    db_card_faces = get_from_db("""
                                SELECT DISTINCT c.source_id
                                FROM [MTG].[CardFace] AS cf
                                JOIN [MTG].[Card] AS c ON c.id = cf.CardID                                
                                """)

    new_card_faces: pd.DataFrame = faces.copy().loc[~faces["id"].isin(db_card_faces["source_id"])]
    if new_card_faces.shape[0] > 0:
        # map the datbase card id to the object 
        db_card_dict = get_from_db("SELECT [ID], [source_id] FROM [MTG].[Card]").set_index("source_id").to_dict()["ID"]
        new_card_faces["id"] = new_card_faces["id"].map(db_card_dict)
        new_card_faces["id"] = new_card_faces["id"].astype("int")
        new_card_faces = new_card_faces.drop(["index"], axis=1)

        new_card_faces = new_card_faces.rename(columns={
            "id": "CardID",
            "cmc": "ConvertedCost",
            "flavor_text": "FlavourText",
            "oracle_id": "OracleID"
        })
        log.info("adding new card faces . . .")


        new_card_faces.to_sql(
            schema="MTG",
            name="CardFace",
            con=engine,
            index=False,
            if_exists="append"
        )
        log.info("new card faces added")
    else:
        log.info("no new card faces found")
    #endregion

    #region Card Part
    log.info("checking for new card parts")
    db_card_parts = get_from_db("""
                                SELECT cid.source_id AS [card_id], [object], [component], cpa.RelatedOracleID AS [related_card]
                                FROM [MTG].[CardPart] AS cpa
                                JOIN [MTG].[Card] AS cid ON cpa.CardID = cid.id
                                """)
    new_card_parts = pd.concat([parts.copy(), db_card_parts]).drop_duplicates(keep=False)

    if "db_card_dict" not in locals():
        db_card_dict: pd.DataFrame = get_from_db("SELECT [ID], [source_id] FROM [MTG].[Card]").set_index("source_id").to_dict()["ID"]

    new_card_parts["card_id"] = new_card_parts["card_id"].map(db_card_dict)

    new_card_parts = new_card_parts.rename(columns={
        "card_id": "CardID",
        "related_card": "RelatedOracleID",
        "component": "Component",
        "object": "Object"
    })

    if new_card_parts.shape[0] > 0:
        log.info("adding new card parts . . .")
        new_card_parts.to_sql(
            schema="MTG",
            name="CardPart",
            con=engine,
            index=False,
            if_exists="append"
        )

        log.info("new card parts added")
    else:
        log.info("no new card parts found")
    #endregion

    #region Card Type Line
    if "db_card_dict" not in locals():
        db_card_dict: pd.DataFrame = get_from_db("SELECT [ID], [source_id] FROM [MTG].[Card]").set_index("source_id").to_dict()["ID"]

    db_card_types: pd.DataFrame = get_from_db("SELECT [id], [name] FROM [MTG].[CardType]").set_index("name").to_dict()["id"]
    db_type_lines: pd.DataFrame = get_from_db("SELECT [card_id], [type_id], [order] FROM [MTG].[TypeLine]")
    if type_lines.shape[0] > 0:
        card_to_type: pd.DataFrame = type_lines.copy()
        card_to_type["order"] = card_to_type.groupby("id").cumcount().add(1)
        card_to_type["id"] = card_to_type["id"].map(db_card_dict)
        card_to_type["type_id"] = card_to_type["type_name"].map(db_card_types)
        card_to_type = card_to_type.drop(["type_name"], axis=1)
        card_to_type = card_to_type.rename(columns={
            "id":"card_id",
            "type_name":"type_id"
        })
        new_type_lines: pd.DataFrame = pd.concat([db_type_lines, card_to_type]).drop_duplicates(keep=False)

        log.info("adding new card type lines . . .")
        new_type_lines.to_sql(
            schema="MTG",
            name="TypeLine",
            con=engine,
            index=False,
            if_exists="append"
        )

        log.info("new card type lines added")
    #endregion
    
    log.info("finished loading data")

if __name__ == "__main__":
    load_dotenv()
    try:
        BULK_NAME = getenv("TCGCT_BULK_NAME")
        LOG_LEVEL = int(getenv('TCGCT_LOG_LEVEL'))
        LOAD_STRAT = str(getenv("TCGCT_LOAD_STRAT"))
        DB_NAME = getenv("TCGCT_DB_NAME")    
        CONN = getenv("TCGCT_CONNECTION_STRING").replace("##DB_NAME##", DB_NAME)
    except Exception as ex:
        print("something went wrong when getting env : "+ex)

    if LOG_LEVEL is None:
        LOG_LEVEL = 20

    if BULK_NAME is None:
        BULK_NAME = "data/bulk_data.json"

    if not path.isdir('logs'):
        mkdir('logs/')

    LOG_TO_CONSOLE = False

    if LOG_TO_CONSOLE:
        lo.basicConfig(level=lo._levelToName[LOG_LEVEL],
                        format='%(asctime)s | %(levelname)s | Line:%(lineno)s | %(message)s',
                        )
    else:
        lo.basicConfig(level=lo._levelToName[LOG_LEVEL],
                        filename='logs/'+str(dt.datetime.today().date())+'.txt',
                        format='%(asctime)s | %(levelname)s | Line:%(lineno)s | %(message)s',
                        filemode='a'
                        )

    if CONN is None:
        exit_as_failed("no connection string defined")

    if DB_NAME is None:
        exit_as_failed("no db name defined")

    if LOAD_STRAT is None or LOAD_STRAT not in ["LOCAL", "DOWNLOAD", "API"]:
        exit_as_failed("No LOAD_STRAT defined")

    log = lo.getLogger(__name__)


    for foreignLogger in lo.Logger.manager.loggerDict:
        if foreignLogger not in [__name__]:
            lo.getLogger(foreignLogger).disabled = True

    engine = create_connection(CONN, DB_NAME)

    log.info("loader started")

    try:
        extract_cards, raw_sets, sets_info = extract()
        raw_cards = mt.prepare_cards(extract_cards)
        cards, faces, parts, type_lines, types, rarities, layouts, sets = transform(raw_cards, raw_sets)
        save_to_db(cards, sets, faces, parts, type_lines, types, rarities, layouts, sets_info)
    except Exception as ex:
        exit_as_failed("unhandled error occurred : " + str(ex))
