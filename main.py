import sqlalchemy as sa
import pandas as pd
import numpy as np
import logging
import datetime as dt
import requests
import json
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

def get_from_db(sql):
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
            bulk_sets = card_frame.loc[:, ["set", "set_name", "set_type", "set_search_uri", "set_id"]].drop_duplicates(keep="first")
            new_sets = bulk_sets.loc[~bulk_sets["set_id"].isin(db_sets["source_id"]), :]
        else:
            api_sets = requests.get("https://api.scryfall.com/sets").json()
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
                print(uri)
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
    # This is done again below ?
    # card_types_lookup = new_cards["type_line"].str.split(" ").explode().drop_duplicates()
    # this then needs to be transformed into a list of the "id" column of each json object
    card_parts = new_cards.loc[new_cards["all_parts"].isnull() == False, ["id", "all_parts"]]

    # Card Type Line
    # 1. Seperate card types by space, then explode, maintain card id
    # 2. join cards with type_line exploded to get them seperated
    card_typeline = new_cards.loc[:, ["id","type_line"]]
    card_typeline_lookup = card_typeline["type_line"].str.split(" ").explode()
    card_to_type_premap = pd.merge(card_typeline, card_typeline_lookup, left_index=True, right_index=True).loc[:, ["id", "type_line_y"]]

    # Card Faces
    card_faces_raw = new_cards.loc[new_cards["card_faces"].notna(), ["id", "card_faces"]]
    face_explode = card_faces_raw.explode("card_faces")
    faces_norm = pd.json_normalize(face_explode["card_faces"])
    with_id = pd.merge(face_explode, faces_norm, left_index=True, right_index=True)
    card_faces = with_id.loc[:, ["id", "object", "name", "image_uris.normal", "mana_cost", "oracle_text", "flavor_text", "layout", "oracle_id", "power", "toughness"]]

    # Card Parts
    card_parts_start = new_cards.loc[new_cards["all_parts"].notna() ,["id", "all_parts"]]
    parts_explode = card_parts_start.explode("all_parts")
    nested_parts = pd.DataFrame.from_dict(parts_explode["all_parts"])
    nested_parts["object"] = nested_parts["all_parts"].str["object"]
    nested_parts["component"] = nested_parts["all_parts"].str["component"]
    nested_parts["id"] = nested_parts["all_parts"].str["id"]

    card_parts = pd.merge(card_parts_start, nested_parts, left_index=True, right_index=True)
    card_parts = card_parts.rename(columns={"id_x":"card_id", "id_y":"related_card"})
    card_parts = card_parts.drop(["all_parts_x", "all_parts_y"], axis=1)

    # Cards
    cards = new_cards.loc[:, ["name", "mana_cost", "oracle_text", "flavor_text", "artist", "collector_number",
                              "power", "toughness", "set", "id", "cmc", "oracle_id", "rarity", "layout"]]


    cards_no_multi = cards.loc[~cards["card_faces"].notna(), ["id", "image_uris"]]
    images = pd.json_normalize(cards_no_multi["image_uris"])
    cards = pd.merge(cards, images["normal"], left_index=True, right_index=True)


    # rename columns to match db
    pass

def load():
    log.info("Beginning load . . .")
    # build lookup
    # new_cards_frame["type_line"].str.split(" ").explode().drop_duplicates().reset_index(drop=True).to_dict()
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
    # this needs to also pass set data
    new_cards_frame, new_sets_frame  = extract()
    print(new_cards_frame["id"].dtype)
    transform(new_cards_frame, new_sets_frame)
    # load()
