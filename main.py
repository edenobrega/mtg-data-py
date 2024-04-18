import sqlalchemy as sa
import pandas as pd
import numpy as np
import logging
import datetime as dt
import requests
import json
import mtg_transform as mt
from sys import exit
from os import mkdir, path, getenv, remove
from dotenv import load_dotenv
from time import sleep

def exit_as_failed():
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

def extract():
    log.info("Beginning extract . . .")
    try:
        db_sets = get_from_db("SELECT [shorthand], [icon], [source_id], [release_date] FROM [MTG].[Set]")
        db_cards = get_from_db("SELECT [source_id] FROM [MTG].[Card]")

        if LOAD_FROM_LOCAL_BULK:
            log.info("loading from bulk data file")
            if not path.exists(BULK_NAME):
                log.critical("bulk file does not exist")
                exit_as_failed()
            card_frame = pd.read_json(BULK_NAME, orient='records')
            log.info("finished loading from bulk data file")
            bulk_sets = card_frame.loc[:, ["set", "set_name", "set_type", "set_search_uri", "set_id"]].drop_duplicates(keep="first")
            new_sets = bulk_sets.loc[~bulk_sets["set_id"].isin(db_sets["source_id"]), :]
        else:
            sets_api_uri = "https://api.scryfall.com/sets"
            log.info("requesting sets data from %s", sets_api_uri)

            sets_req: requests.Response = requests.get(sets_api_uri)
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
            new_sets = api_frame.loc[~api_frame["id"].isin(db_sets["source_id"]), :]
            log.info("%s new sets found", new_sets.shape[0])

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
    except Exception as ex:
        log.exception("error during extract")
        exit()
    # Filter cards that dont exist in db
    card_frame = card_frame.loc[~card_frame["id"].isin(db_cards["source_id"]), :]
    log.info("Finished Extract")
    return card_frame, new_sets

def transform(new_cards, new_sets):
    log.info("Beginning transform . . .")
    
    set_types = new_sets["set_type"].drop_duplicates()
    if LOAD_FROM_LOCAL_BULK:
        # CERTAIN COLUMNS FOR SET DONT EXIST IF LOADED FROM BULK
        sets = new_sets.loc[:, ["set_name", "set", "set_search_uri", "set_type", "set_id"]]
    else:
        sets: pd.DataFrame = new_sets.loc[:, ["name", "code", "search_uri", "set_type", "id"]]
        sets = sets.rename(columns={"name":"set_name", "code":"set", "search_uri":"set_search_uri", "id":"set_id"})

    if new_cards.shape[0] == 0:
        log.info("no new cards found")
        return {
            "no_cards": True,
            "sets": sets,
            "set_types": set_types
        }

    rarities = new_cards["rarity"].drop_duplicates()
    layouts = new_cards["layout"].drop_duplicates()

    type_line = mt.get_type_line_data(new_cards)
    card_faces = mt.get_card_faces(new_cards)
    card_parts = mt.get_card_parts(new_cards)

    # Cards
    log.info("transforming cards . . .")
    if "card_faces" not in new_cards:
        new_cards["card_faces"] = pd.NA
    cards = new_cards.loc[:, ["name", "mana_cost", "oracle_text", "flavor_text", "artist", "collector_number",
                              "power", "toughness", "set", "id", "cmc", "oracle_id", "rarity", "layout", "card_faces", "image_uris"]]

    cards_no_multi: pd.DataFrame = cards.loc[~cards["card_faces"].notna(), ["id", "image_uris"]]
    if cards_no_multi.empty == False:
        images = pd.json_normalize(cards_no_multi["image_uris"])
        cards = pd.merge(cards, images["normal"], left_index=True, right_index=True, how="left")
    cards = cards.drop(["image_uris", "card_faces"], axis=1)
    
    log.info("Finished transforming cards")
    
    log.info("Finished transform")
    return {
        "cards": cards,
        "card_faces": card_faces,
        "card_parts": card_parts,
        "rarities": rarities,
        "layouts": layouts,
        "type_line": type_line,
        "sets": sets,
        "set_types": set_types
    }

# FIXME: Replace use of BCPandas with just the library, due to there be lots of strange interactions with how it pushes data to db
#           also it would just be interesting
#           https://bcp.readthedocs.io/en/latest/
def load(data: dict):
    log.info("Beginning load . . .")
    
    # Set Type
    log.info("checking for new set types")  
    tf_settypes = data["set_types"].copy().to_frame(name="name")
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
    sets_source_ids = data["sets"].copy().drop_duplicates()
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

    if "no_cards" in data:
        log.info("no new card data to load")
        return

    # Rarity
    log.info("checking for new rarities")
    db_rarities = get_from_db("select [name] from [MTG].[Rarity]")
    new_rarities = data["rarities"].copy().loc[~data["rarities"].isin(db_rarities["name"])]
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

    # Layout
    log.info("checking for new layouts")
    db_layouts = get_from_db("select [name] from [MTG].[Layout]")
    new_layouts = data["layouts"].copy().loc[~data["layouts"].isin(db_layouts["name"])]
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

    # Card
    log.info("checking for new cards")
    db_card_ids = get_from_db("SELECT [source_id], [id] FROM [MTG].[Card]")
    new_cards: pd.DataFrame = data["cards"].copy().loc[~data["cards"]["id"].isin(db_card_ids["source_id"])]
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
        # bcpandas automatically assigns nan to ""
        # TODO: check if the non bcpandas insert does the same
        new_cards["power"] = new_cards["power"].astype("str").map({"nan":""})
        new_cards["toughness"] = new_cards["toughness"].astype("str").map({"nan":""})

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

    # Card Face
    db_card_faces = get_from_db("SELECT [CardID] FROM [MTG].[CardFace]")
    # because these are nested in a obj, you would have to reload a card again to check if its face exists, so the chances are we dont need to check for duplicates, but will do so anyway
    #       in case of trying to reload specific cards by deleting them from the db  i guess
    new_card_faces: pd.DataFrame = data["card_faces"].copy().loc[~data["card_faces"]["id"].isin(db_card_faces["CardID"])]
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

    # Card Part
    log.info("checking for new card parts")
    db_card_parts = get_from_db("""
                                SELECT cid.source_id AS [card_id], [object], [component], rci.source_id AS [related_card]
                                FROM [MTG].[CardPart] AS cpa
                                JOIN [MTG].[Card] AS cid ON cpa.CardID = cid.id
                                JOIN [MTG].[Card] AS rci ON rci.id = cpa.RelatedCardID
                                """)
    new_card_parts = pd.concat([data["card_parts"].copy(), db_card_parts]).drop_duplicates(keep=False)

    if "db_card_dict" not in locals():
        db_card_dict: pd.DataFrame = get_from_db("SELECT [ID], [source_id] FROM [MTG].[Card]").set_index("source_id").to_dict()["ID"]

    new_card_parts["card_id"] = new_card_parts["card_id"].map(db_card_dict)
    new_card_parts["related_card"] = new_card_parts["related_card"].map(db_card_dict)

    # bc_to_sql requires exact match of column names
    new_card_parts = new_card_parts.rename(columns={
        "card_id": "CardID",
        "related_card": "RelatedCardID",
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

    # Card Types
    db_card_types = get_from_db("SELECT [name] FROM [MTG].[CardType]")
    new_card_types: pd.DataFrame = data["type_line"].lookup.loc[~data["type_line"].lookup["type_line"].isin(db_card_types["name"])].copy()
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

    # Card Type Line
    if "db_card_dict" not in locals():
        db_card_dict: pd.DataFrame = get_from_db("SELECT [ID], [source_id] FROM [MTG].[Card]").set_index("source_id").to_dict()["ID"]

    db_card_types: pd.DataFrame = get_from_db("SELECT [id], [name] FROM [MTG].[CardType]").set_index("name").to_dict()["id"]
    if data["type_line"].premap.shape[0] > 0:
        card_to_type: pd.DataFrame = data["type_line"].premap.copy()
        card_to_type["order"] = card_to_type.groupby("id").cumcount().add(1)
        card_to_type["id"] = card_to_type["id"].map(db_card_dict)
        card_to_type["type_id"] = card_to_type["type_name"].map(db_card_types)
        card_to_type = card_to_type.drop(["type_name"], axis=1)
        card_to_type = card_to_type.rename(columns={
            "id":"card_id",
            "type_name":"type_id"
        })

        log.info("adding new card type lines . . .")
        card_to_type.to_sql(
            schema="MTG",
            name="TypeLine",
            con=engine,
            index=False,
            if_exists="append"
        )

        log.info("new card type lines added")
    log.info("finished loading data")

if __name__ == "__main__":
    load_dotenv()
    BULK_NAME = "bulk_download.json"
    # BULK_NAME = "test_data.json"

    if not path.isdir('logs'):
        mkdir('logs\\')

    logging.basicConfig(level=logging._levelToName[int(getenv('TCGCT_LOG_LEVEL'))],
                    filename='logs\\'+str(dt.datetime.today().date())+'.txt',
                    format='%(asctime)s | %(levelname)s | Line:%(lineno)s | %(message)s',
                    filemode='a')
    log = logging.getLogger(__name__)

    for foreignLogger in logging.Logger.manager.loggerDict:
        if foreignLogger not in [__name__, 'mtg_transform']:
            logging.getLogger(foreignLogger).disabled = True

    log.info("main.py started")

    if getenv("TCGCT_DOWNLOAD_BULK").upper() == "TRUE":
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
            remove(BULK_NAME)

        with open(BULK_NAME, 'x', encoding='utf-8') as f:
            json.dump(bulk_data, f, ensure_ascii=False, indent=4)   

    LOAD_FROM_LOCAL_BULK = getenv("TCGCT_LOAD_FROM_LOCAL_BULK").upper() == "TRUE"
    DB_NAME = getenv("TCGCT_DB_NAME")    
    CONN = getenv("TCGCT_CONNECTION_STRING").replace("##DB_NAME##", DB_NAME)

    if CONN is None:
        log.critical("no connection string defined")
        exit_as_failed()

    if DB_NAME is None:
        log.critical("no db name defined")
        exit_as_failed()

    engine = sa.create_engine(CONN)
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
            sql = sql.bindparams(db_name=DB_NAME)
            schema_check = conn.execute(sql).one_or_none()
            if schema_check is None:
                log.critical("tables are missing from the schema")
                exit_as_failed()
            log.debug("tables exist")
    except Exception as ex:
        log.critical("failed to connect to db : %s", ex)
        exit_as_failed()
    
    # Get raw data, and get data for sets where our card count mismatch what the api gives us.
    #       This is because cards for a new set are slowly revelead to us, and they are updated
    #       in the api one by one, there is probably a more efficient way of doing this.
    new_cards_frame, new_sets_frame = extract()

    # Transform data into frames that require minimal transformation before loading
    data = transform(new_cards_frame, new_sets_frame)

    # Remove existing data from dataframes and then push to the db, then get certain tables back
    #       to create lookups and replace values in the dataframes to match this.
    load(data)

