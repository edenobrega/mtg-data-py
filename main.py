import sqlalchemy as sa
import pandas as pd
import numpy as np
import logging
import datetime as dt
import requests
import json
import mtg_transform as mt
from bcpandas import to_sql as bc_to_sql, SqlCreds
from sys import exit
from os import mkdir, path, getenv, remove
from dotenv import load_dotenv
from time import sleep
# load new sets into db
# onces thats done load it back into pandas
# get new ids from there

card_to_type_premap = []

# data = pd.read_sql("select * from [MTG].[Rarity]", engine)

# raw = {
#     "name": ["rare","mythic","common"]
# }
# new_data = pd.DataFrame(raw)

# data = data.loc[:, ["name"]]

# new_rarities = pd.concat([new_data, data]).drop_duplicates(["name"], keep=False)


# x = new_rarities.to_sql(
#     schema="MTG",
#     name="Rarity",
#     con=engine,
#     index=False,
#     if_exists="append"
# )

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

# Load all from bulk file
# or
# check api to see if new set has released
# check the card count for a set against the card count in the db
def extract():
    log.info("Beginning extract . . .")
    try:
        db_sets = get_from_db("SELECT [shorthand], [icon], [source_id], [release_date] FROM [MTG].[Set]")
        db_cards = get_from_db("SELECT [source_id] FROM [MTG].[Card]")

        if LOAD_FROM_BULK:
            log.info("loading from bulk data file")
            if not path.exists(bulk_name):
                log.critical("file does not exist")
                exit()
            card_frame = pd.read_json(bulk_name, orient='records')
            log.info("finished loading from bulk data file")
            bulk_sets = card_frame.loc[:, ["set", "set_name", "set_type", "set_search_uri", "set_id"]].drop_duplicates(keep="first")
            new_sets = bulk_sets.loc[~bulk_sets["set_id"].isin(db_sets["source_id"]), :]
        else:
            sets_api_uri = "https://api.scryfall.com/sets"
            log.info("requesting sets data from %s", sets_api_uri)
            # TODO: handle potential errors
            api_sets = requests.get(sets_api_uri).json()
            api_frame = pd.DataFrame.from_dict(api_sets["data"])

            # Get all sets that dont exist in the db
            #       add missing to the update list
            new_sets = api_frame.loc[~api_frame["id"].isin(db_sets["source_id"]), :]
            log.debug("%s new sets found", new_sets.shape[0])

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

            raw_card_data = []
            for uri in needs_update.loc[needs_update["card_count"] > 0, :]["search_uri"]:
                cards = request_set_cards(uri, [])
                raw_card_data = raw_card_data + cards

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
    if LOAD_FROM_BULK:
        # CERTAIN COLUMNS FOR SET DONT EXIST IF LOADED FROM BULK
        sets = new_sets.loc[:, ["set_name", "set", "set_search_uri", "set_type", "set_id"]]
        pass
    else:
        raise Exception("not yet implemented")

    rarities = new_cards["rarity"].drop_duplicates()
    layouts = new_cards["layout"].drop_duplicates()

    type_line = mt.get_type_line_data(new_cards)
    card_faces = mt.get_card_faces(new_cards)
    card_parts = mt.get_card_parts(new_cards)

    # Cards
    cards = new_cards.loc[:, ["name", "mana_cost", "oracle_text", "flavor_text", "artist", "collector_number",
                              "power", "toughness", "set", "id", "cmc", "oracle_id", "rarity", "layout", "card_faces", "image_uris"]]

    cards_no_multi = cards.loc[~cards["card_faces"].notna(), ["id", "image_uris"]]
    images = pd.json_normalize(cards_no_multi["image_uris"])
    cards = pd.merge(cards, images["normal"], left_index=True, right_index=True, how="left")
    cards = cards.drop(["image_uris", "card_faces"], axis=1)

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

def load(data: dict):
    log.info("Beginning load . . .")
    
    # Set Type
    # TODO: is the drop needed?
    log.info("checking for new set types")  
    tf_settypes = data["set_types"].to_frame(name="name")      
    db_settypes = get_from_db("SELECT [name] FROM [MTG].[SetType]")
    new_settypes = tf_settypes.loc[~tf_settypes["name"].isin(db_settypes["name"]), :]
    result = new_settypes.to_sql(
        schema="MTG",
        name="SetType",
        con=engine,
        index=False,
        if_exists="append"
    )
    log.info("%s new set types added", result)

    # Sets
    log.info("checking for new sets")
    sets_source_ids = data["sets"].drop_duplicates()
    db_sets = get_from_db("select [source_id] from mtg.[Set]")
    new_sets: pd.DataFrame = sets_source_ids.loc[~sets_source_ids["set_id"].isin(db_sets["source_id"])]
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
    new_sets["set_type_id"] = new_sets["set_type_id"].replace(settype_lookup)
    new_sets["set_type_id"] = new_sets["set_type_id"].astype("int")
    result = new_sets.to_sql(
        schema="MTG",
        name="Set",
        con=engine,
        index=False,
        if_exists="append"
    )
    log.info("%s new sets added", result)

    # Rarity
    log.info("checking for new rarities")
    db_rarities = get_from_db("select [name] from [MTG].[Rarity]")
    new_rarities = data["rarities"].loc[~data["rarities"].isin(db_rarities["name"])]
    new_rarities.name = "name"
    result = new_rarities.to_sql(
        schema="MTG",
        name="Rarity",
        con=engine,
        index=False,
        if_exists="append"
    )
    log.info("%s new rarities added", result)

    # Layout
    log.info("checking for new layouts")
    db_layouts = get_from_db("select [name] from [MTG].[Layout]")
    new_layouts = data["layouts"].loc[~data["layouts"].isin(db_layouts["name"])]
    new_layouts.name = "name"
    result = new_layouts.to_sql(
        schema="MTG",
        name="Layout",
        con=engine,
        index=False,
        if_exists="append"
    )
    log.info("%s new layouts added", result)

    # Card
    log.info("checking for new cards")
    db_card_ids = get_from_db("SELECT [source_id] FROM [MTG].[Card]")
    new_cards: pd.DataFrame = data["cards"].loc[~data["cards"]["id"].isin(db_card_ids["source_id"])]

    rarity_lookup = get_from_db("select [id], [name] from [MTG].[Rarity]").set_index("name")
    layout_lookup = get_from_db("select [id], [name] from [MTG].[Layout]").set_index("name")
    set_lookup = get_from_db("SELECT [id], [shorthand] FROM [MTG].[Set]").set_index("shorthand")
    
    rarity_lookup["id"] = rarity_lookup["id"].astype("str")
    layout_lookup["id"] = layout_lookup["id"].astype("str")
    set_lookup["id"] = set_lookup["id"].astype("str")

    rarity_lookup = rarity_lookup.to_dict()["id"]
    layout_lookup = layout_lookup.to_dict()["id"]
    set_lookup = set_lookup.to_dict()["id"]

    new_cards["rarity"] = new_cards["rarity"].replace(rarity_lookup)
    new_cards["layout"] = new_cards["layout"].replace(layout_lookup)
    new_cards["set"] = new_cards["set"].replace(set_lookup)

    new_cards["rarity"] = new_cards["rarity"].astype("int")
    new_cards["layout"] = new_cards["layout"].astype("int")
    new_cards["set"] = new_cards["set"].astype("int")

    new_cards["collector_number"] = new_cards["collector_number"].astype("str")
    new_cards["power"] = new_cards["power"].astype("str")
    new_cards["toughness"] = new_cards["toughness"].astype("str")

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

    creds = SqlCreds.from_engine(engine)

    bc_to_sql(
        df=new_cards,
        schema="MTG",
        table_name="Card",
        index=False,
        if_exists="append",
        creds=creds
    )
    log.info("%s new cards added", new_cards.shape[0])
    
    # TODO: only occurred to me now but, because faces and parts are nested, there is no way to figure out if a card did not load faces/parts properly without just loading it all again
    # Card Face
    # card_faces
    new_card_faces = data["card_faces"]
    db_card_faces = get_from_db("SELECT [CardID], [OracleID] FROM [MTG].[CardFace]")
    # Card Part

    # Card Type

    # Type Line






    # Get all unique types

    # Check these against currently in the db

    # Add currently not existing ones to db

    # Get all from db

    # Get all types with linked card id

    # mapping shennanigans 

    pass


data_frame = None
if __name__ == "__main__":
    load_dotenv()
    bulk_name = "bulk_download.json"

    if not path.isdir('logs'):
        mkdir('logs\\')

    logging.basicConfig(level=logging._levelToName[int(getenv('TCGCT_LOG_LEVEL'))],
                    filename='logs\\'+str(dt.datetime.today().date())+'.txt',
                    format='%(asctime)s | %(levelname)s | Line:%(lineno)s | %(message)s',
                    filemode='a')
    log = logging.getLogger(__name__)

    for foreignLogger in logging.Logger.manager.loggerDict:
        if foreignLogger != __name__:
            logging.getLogger(foreignLogger).disabled = True

    if getenv("TCGCT_DOWNLOAD_BULK").upper() == "TRUE":
        log.debug("downloading bulk data")

        bulk_catalog = "https://api.scryfall.com/bulk-data"
        catalog = requests.get(bulk_catalog).json()
        bulk_uri = next(obj["download_uri"] for obj in catalog["data"] if obj["type"] == "default_cards")
        bulk_data = requests.get(bulk_uri).json()

        if path.exists(bulk_name):
            remove(bulk_name)

        with open(bulk_name, 'x', encoding='utf-8') as f:
            json.dump(bulk_data, f, ensure_ascii=False, indent=4)   

    LOAD_FROM_BULK = getenv("TCGCT_LOAD_FROM_BULK").upper() == "TRUE"

    engine = sa.create_engine("mssql+pyodbc://DESKTOP-UPNS42E\\SQLEXPRESS/tcgct-dev?driver=ODBC+Driver+17+for+SQL+Server")
    # Get raw data, and get data for sets where our card count mismatch what the api gives us.
    #       This is because cards for a new set are slowly revelead to us, and they are updated
    #       in the api one by one, there is probably a more efficient way of doing this.
    new_cards_frame, new_sets_frame  = extract()

    # Transform data into frames that require minimal transformation before loading
    data = transform(new_cards_frame, new_sets_frame)

    # Remove existing data from dataframes and then push to the db, then get certain tables back
    #       to create lookups and replace values in the dataframes to match this.
    # load(data)
