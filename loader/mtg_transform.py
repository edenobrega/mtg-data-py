import pandas as pd
import numpy as np
import logging
log = logging.getLogger("__main__")

def prepare_cards(_cards: pd.DataFrame) -> pd.DataFrame:
    """Add all potentially missing columns to provided cards dataframe
    
    Parameters:
    _cards (pd.DataFrame): Cards frame
    """
    column_check = ["name", "mana_cost", "oracle_text", "flavor_text", "artist", "collector_number",
                              "power", "toughness", "set", "id", "cmc", "oracle_id", "rarity", "layout", "card_faces", "image_uris", "loyalty", "type_line", "all_parts"]
    ret_cards = _cards.reindex(_cards.columns.union(column_check, sort=False), axis=1, fill_value=pd.NA)
    return ret_cards.loc[:, column_check]

def get_card_faces(cards: pd.DataFrame) -> pd.DataFrame:
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

    column_types = {"id":"str", "object":"str", "name":"str", "cmc":"Int64", "oracle_id":"str", "loyalty":"float64", "normal":"str"}

    card_faces_single = card_faces_single.astype(column_types)
    card_faces = pd.concat([card_faces_multi,card_faces_single])
    
    log.debug("merging image columns")
    # Merge image columns into a single one.
    card_faces = card_faces.reset_index()
    card_faces["image"] = card_faces["normal"]
    card_faces.loc[card_faces["image"].isna(), "image"] = card_faces["image_uris.normal"] 
    card_faces.drop(["normal","image_uris.normal"], axis=1, inplace=True)

    card_faces["power"] = np.where(pd.isnull(card_faces["power"]),card_faces["power"],card_faces["power"].astype("str"))
    card_faces["toughness"] = np.where(pd.isnull(card_faces["toughness"]),card_faces["toughness"],card_faces["toughness"].astype("str"))
    card_faces["loyalty"] = np.where(pd.isnull(card_faces["loyalty"]),card_faces["loyalty"],card_faces["loyalty"].astype("str"))
    card_faces["flavor_text"] = np.where(pd.isnull(card_faces["flavor_text"]),card_faces["flavor_text"],card_faces["flavor_text"].astype("str"))

    card_faces.loc[card_faces["oracle_text"] == "", "oracle_text"] = None
    card_faces.loc[card_faces["mana_cost"] == "", "mana_cost"] = None

    log.info("finished preparing card faces")
    return card_faces

def get_card_parts(cards: pd.DataFrame) -> pd.DataFrame:
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

def get_rarities(cards: pd.DataFrame) -> pd.Series:
    return cards["rarity"].drop_duplicates()

def get_layouts(cards: pd.DataFrame) -> pd.Series:
    return cards["layout"].drop_duplicates()

def get_cards(_cards: pd.DataFrame) -> pd.DataFrame:
    if "card_faces" not in _cards:
        _cards["card_faces"] = pd.NA
    cards = _cards.loc[:, ["name", "mana_cost", "oracle_text", "flavor_text", "artist", "collector_number",
                              "power", "toughness", "set", "id", "cmc", "oracle_id", "rarity", "layout", "card_faces", "image_uris", "loyalty"]]

    cards_no_multi: pd.DataFrame = cards.loc[cards["card_faces"].isna(), ["id", "image_uris"]]
    if cards_no_multi.empty == False:
        images = pd.json_normalize(cards_no_multi["image_uris"]).set_index(cards_no_multi.index)
        cards = pd.merge(cards, images["normal"], left_index=True, right_index=True, how="left")
    cards = cards.drop(["image_uris", "card_faces"], axis=1)

    return cards
