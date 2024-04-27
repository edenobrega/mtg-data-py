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

#region Large Transforms
# Some cards with two faces only have one image, and those images will be in the usual
def get_card_faces(cards: pd.DataFrame):
    log.info("preparing card faces")
    # Cards with actual multiple faces
    column_check = ["id", "object", "name", "normal", "mana_cost", "oracle_text", "cmc", "flavor_text", "loyalty", "oracle_id", "power", "toughness"]
    column_check_multi = ["id", "object", "name", "image_uris.normal", "mana_cost", "oracle_text", "cmc", "flavor_text", "loyalty", "oracle_id", "power", "toughness"]
    if "card_faces" in cards:
        card_faces_multi_raw = cards.loc[(cards["card_faces"].notna()) & (cards["image_uris"].isna()), ["id", "card_faces", "oracle_id"]]
        if card_faces_multi_raw.empty == False:
            face_explode = card_faces_multi_raw.explode("card_faces")
            faces_norm = pd.json_normalize(face_explode["card_faces"]).set_index(face_explode.index)
            with_id = pd.merge(face_explode, faces_norm, left_index=True, right_index=True, how="left")
            if "oracle_id_x" in with_id:
                with_id["oracle_id_x"] = with_id["oracle_id_x"].fillna(with_id["oracle_id_y"])
                with_id = with_id.drop(["oracle_id_y"], axis=1)
                with_id = with_id.rename(columns={ "oracle_id_x":"oracle_id" })
            with_id = with_id.reindex(with_id.columns.union(column_check_multi, sort=False), axis=1,fill_value=np.nan)
            card_faces_multi = with_id.loc[:, column_check_multi].drop_duplicates()
        else:
            card_faces_multi = pd.DataFrame(columns=column_check_multi)

        # Cards with multiple faces, but one image in root image_uris prop
        column_check = ["id", "object", "name", "normal", "mana_cost", "oracle_text", "cmc", "flavor_text", "loyalty", "oracle_id", "power", "toughness"]
        card_faces_single_raw = cards.loc[(cards["card_faces"].notna()) & (cards["image_uris"].notna()), ["id", "image_uris", "card_faces", "oracle_id"]]
        if card_faces_single_raw.empty == False:
            images_norm = pd.json_normalize(card_faces_single_raw["image_uris"]).set_index(card_faces_single_raw.index).loc[:, ["normal"]]
            with_image = pd.merge(card_faces_single_raw, images_norm, left_index=True, right_index=True)
            face_explode = with_image.explode("card_faces")
            explode_norm = pd.json_normalize(face_explode["card_faces"]).set_index(face_explode.index)
            explode_image_merge = pd.merge(with_image, explode_norm, left_index=True, right_index=True)
            explode_image_merge = explode_image_merge.reindex(explode_image_merge.columns.union(column_check, sort=False), axis=1, fill_value=np.nan)
            card_faces_single = explode_image_merge.loc[:, column_check]
        else:
            card_faces_single: pd.DataFrame = pd.DataFrame(columns=column_check)
    else:
        log.warning("no card faces found")
        return pd.DataFrame(columns=column_check)
    
    if card_faces_multi.empty and card_faces_single.empty:
        log.warning("no card faces found")
        return card_faces_single


    # Convert types to avoid future warning on concat
    log.debug("casting columns")

    card_faces_multi["power"] = np.where(pd.isnull(card_faces_multi["power"]),card_faces_multi["power"],card_faces_multi["power"].astype("str"))
    card_faces_multi["toughness"] = np.where(pd.isnull(card_faces_multi["toughness"]),card_faces_multi["toughness"],card_faces_multi["toughness"].astype("str"))
    card_faces_multi["loyalty"] = np.where(pd.isnull(card_faces_multi["loyalty"]),card_faces_multi["loyalty"],card_faces_multi["loyalty"].astype("str"))
    card_faces_multi["flavor_text"] = np.where(pd.isnull(card_faces_multi["flavor_text"]),card_faces_multi["flavor_text"],card_faces_multi["flavor_text"].astype("str"))

    card_faces_multi.loc[card_faces_multi["oracle_text"] == "", "oracle_text"] = pd.NA
    card_faces_multi.loc[card_faces_multi["mana_cost"] == "", "mana_cost"] = pd.NA

    column_types = {"id":"str", "object":"str", "name":"str", "image_uris.normal":"str", "cmc":"Int64", "oracle_id":"str"}
    card_faces_multi = card_faces_multi.astype(column_types)
    del column_types["image_uris.normal"]
    column_types["normal"] = "str"
    card_faces_single = card_faces_single.astype(column_types)
    card_faces = pd.concat([card_faces_multi,card_faces_single])
    
    log.debug("merging image columns")
    # Merge image columns into a single one.
    card_faces = card_faces.reset_index()
    card_faces["image"] = card_faces["normal"]
    card_faces.loc[card_faces["image"].isna(), "image"] = card_faces["image_uris.normal"] 
    card_faces.drop(["normal","image_uris.normal"], axis=1, inplace=True)
    log.info("finished preparing card faces")
    return card_faces

def get_card_parts(cards: pd.DataFrame):
    log.info("preparing card parts")
    column_check = ["card_id", "object", "component", "related_card"]
    if "all_parts" not in cards:
        return pd.DataFrame(columns=column_check)
    card_parts_start = cards.loc[cards["all_parts"].notna() ,["id", "all_parts"]]
    parts_explode = card_parts_start.explode("all_parts")
    nested_parts = pd.DataFrame.from_dict(parts_explode["all_parts"])
    nested_parts["object"] = nested_parts["all_parts"].str["object"]
    nested_parts["component"] = nested_parts["all_parts"].str["component"]
    nested_parts["id"] = nested_parts["all_parts"].str["id"]

    card_parts = pd.merge(card_parts_start, nested_parts, left_index=True, right_index=True)
    card_parts = card_parts.rename(columns={"id_x":"card_id", "id_y":"related_card"})
    card_parts = card_parts.drop(["all_parts_x", "all_parts_y"], axis=1)

    log.info("finished preparing card parts")
    return card_parts

def get_type_line_data(cards: pd.DataFrame):
    log.info("preparing type line data")
    card_typeline = cards.loc[cards["type_line"].notna(), ["id","type_line"]].copy()
    card_typeline_lookup = card_typeline["type_line"].str.split(" ").explode()
    card_to_type_premap_root = pd.merge(card_typeline, card_typeline_lookup, left_index=True, right_index=True).loc[:, ["id", "type_line_y"]]
    card_to_type_premap_root = card_to_type_premap_root.rename(columns={"type_line_y":"type_name"})
    card_types_lookup_root = card_typeline_lookup.drop_duplicates().reset_index().drop(["index"],axis=1) 

    # some cards dont have a type_line in its root, and instead its nested in its faces
    if "card_faces" in cards:
        card_no_typeline = cards.loc[cards["type_line"].isna(), ["id","card_faces"]].copy()
        face_explode = card_no_typeline.explode("card_faces")
        nested_faces = pd.json_normalize(face_explode["card_faces"]).set_index(face_explode.index)
        nested_faces = nested_faces.loc[:, ["type_line"]]
        type_line_with_id = nested_faces["type_line"].str.split(" ").explode()
        card_to_type_premap_nested = pd.merge(card_no_typeline["id"], type_line_with_id, left_index=True, right_index=True)
        card_types_lookup_nested = type_line_with_id.drop_duplicates().reset_index().drop(["index"],axis=1) 
        card_types_lookup = pd.concat([card_types_lookup_root, card_types_lookup_nested])
        card_to_type_premap = pd.concat([card_to_type_premap_root, card_to_type_premap_nested])
    else:
        card_to_type_premap = card_to_type_premap
    card_to_type_premap = card_to_type_premap.reset_index()
    card_to_type_premap.loc[card_to_type_premap["type_name"].isna(), "type_name"] = card_to_type_premap["type_line"] 
    card_to_type_premap.drop(["type_line"], axis=1, inplace=True)
    card_to_type_premap.set_index(["index"], inplace=True)

    log.info("finished preparing type line data")
    return card_types_lookup, card_to_type_premap
#endregion

def extract() -> pd.DataFrame:
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

def transform(cards_raw: pd.DataFrame, sets_frame: pd.DataFrame):
    cards: pd.DataFrame = None
    faces: pd.DataFrame = None
    parts: pd.DataFrame = None
    type_lines: pd.DataFrame = None
    types: pd.DataFrame = None
    # TODO: ensure same columns as when comes from api and comes from bulkdata
    sets: pd.DataFrame = sets_frame.copy()
    # sets: pd.DataFrame = None

    rarities = cards_raw["rarity"].drop_duplicates()
    layouts = cards_raw["layout"].drop_duplicates()
    types, type_lines = get_type_line_data(cards_raw)
    faces = get_card_faces(cards_raw)
    parts = get_card_parts(cards_raw)


    if "card_faces" not in cards_raw:
        cards_raw["card_faces"] = pd.NA
    cards = cards_raw.loc[:, ["name", "mana_cost", "oracle_text", "flavor_text", "artist", "collector_number",
                              "power", "toughness", "set", "id", "cmc", "oracle_id", "rarity", "layout", "card_faces", "image_uris"]]

    cards_no_multi: pd.DataFrame = cards.loc[cards["card_faces"].isna(), ["id", "image_uris"]]
    if cards_no_multi.empty == False:
        images = pd.json_normalize(cards_no_multi["image_uris"]).set_index(cards_no_multi.index)
        cards = pd.merge(cards, images["normal"], left_index=True, right_index=True, how="left")
    cards = cards.drop(["image_uris", "card_faces"], axis=1)

    return cards, faces, parts, type_lines, types, rarities, layouts, sets_frame

# TODO: Make bulk inserts faster
#       Potentionally use https://bcp.readthedocs.io/en/latest/
def save_to_db(cards: pd.DataFrame, sets: pd.DataFrame, faces: pd.DataFrame, parts: pd.DataFrame, type_lines: pd.DataFrame, types: pd.DataFrame, rarities: pd.Series, layouts: pd.Series):
    log.info("Beginning load . . .")
    
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

        new_cards.loc[new_cards["mana_cost"] == "", "mana_cost"] = pd.NA

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
    db_card_faces = get_from_db("SELECT [CardID] FROM [MTG].[CardFace]")
    # because these are nested in a obj, you would have to reload a card again to check if its face exists, so the chances are we dont need to check for duplicates, but will do so anyway
    #       in case of trying to reload specific cards by deleting them from the db  i guess
    new_card_faces: pd.DataFrame = faces.copy().loc[~faces["id"].isin(db_card_faces["CardID"])]
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
                                SELECT cid.source_id AS [card_id], [object], [component], rci.source_id AS [related_card]
                                FROM [MTG].[CardPart] AS cpa
                                JOIN [MTG].[Card] AS cid ON cpa.CardID = cid.id
                                JOIN [MTG].[Card] AS rci ON rci.id = cpa.RelatedCardID
                                """)
    new_card_parts = pd.concat([parts.copy(), db_card_parts]).drop_duplicates(keep=False)

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
    #endregion

    #region Card Type Line
    if "db_card_dict" not in locals():
        db_card_dict: pd.DataFrame = get_from_db("SELECT [ID], [source_id] FROM [MTG].[Card]").set_index("source_id").to_dict()["ID"]

    db_card_types: pd.DataFrame = get_from_db("SELECT [id], [name] FROM [MTG].[CardType]").set_index("name").to_dict()["id"]
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

        log.info("adding new card type lines . . .")
        card_to_type.to_sql(
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

    engine = create_connection(CONN, DB_NAME)

    log.info("loader started")

    cards_raw, sets_raw = extract()
    
    cards, faces, parts, type_lines, types, rarities, layouts, sets = transform(cards_raw, sets_raw)

    save_to_db(cards, sets, faces, parts, type_lines, types, rarities, layouts)
