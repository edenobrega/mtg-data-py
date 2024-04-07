import pandas as pd
pd.set_option('display.max_colwidth', 100)

# Some cards with two faces only have one image, and those images will be in the usual
def get_card_faces(cards: pd.DataFrame):
    # Cards with actual multiple faces
    column_check = ["id", "object", "name", "image_uris.normal", "mana_cost", "oracle_text", "cmc", "flavor_text", "layout", "loyalty", "oracle_id", "power", "toughness"]
    card_faces_multi_raw = cards.loc[(cards["card_faces"].notna()) & (cards["image_uris"].isna()), ["id", "card_faces"]]
    face_explode = card_faces_multi_raw.explode("card_faces")
    faces_norm = pd.json_normalize(face_explode["card_faces"]).set_index(face_explode.index)
    with_id = pd.merge(face_explode, faces_norm, left_index=True, right_index=True)
    with_id = with_id.reindex(with_id.columns.union(column_check, sort=False), axis=1,fill_value=pd.NA)
    card_faces_multi = with_id.loc[:, column_check].drop_duplicates()

    # Cards with multiple faces, but one image in root image_uris prop
    column_check = ["id", "object", "name", "normal", "mana_cost", "oracle_text", "cmc", "flavor_text", "layout", "loyalty", "oracle_id", "power", "toughness"]
    card_faces_single_raw = cards.loc[(cards["card_faces"].notna()) & (cards["image_uris"].notna()), ["id", "image_uris", "card_faces"]]
    images_norm = pd.json_normalize(card_faces_single_raw["image_uris"]).set_index(card_faces_single_raw.index).loc[:, ["normal"]]
    with_image = pd.merge(card_faces_single_raw, images_norm, left_index=True, right_index=True)
    face_explode = with_image.explode("card_faces")
    explode_norm = pd.json_normalize(face_explode["card_faces"]).set_index(face_explode.index)
    explode_image_merge = pd.merge(with_image, explode_norm, left_index=True, right_index=True)
    explode_image_merge = explode_image_merge.reindex(explode_image_merge.columns.union(column_check, sort=False), axis=1, fill_value=pd.NA)
    card_faces_single = explode_image_merge.loc[:, column_check]

    # Convert types to avoid future warning on concat
    column_types = {"id":"str", "object":"str", "name":"str", "image_uris.normal":"str", "mana_cost":"str", "oracle_text":"str", "cmc":"str", "flavor_text":"str", "layout":"str", "loyalty":"str", "oracle_id":"str", "power":"str", "toughness":"str"}
    card_faces_multi = card_faces_multi.astype(column_types)
    del column_types["image_uris.normal"]
    column_types["normal"] = "str"
    card_faces_single = card_faces_single.astype(column_types)
    card_faces = pd.concat([card_faces_multi,card_faces_single])

    # Merge image columns into a single one.
    card_faces = card_faces.reset_index()
    card_faces["image"] = card_faces["normal"]
    card_faces.loc[card_faces["image"].isna(), "image"] = card_faces["image_uris.normal"] 
    card_faces.drop(["normal","image_uris.normal"], axis=1, inplace=True)
    return card_faces

# Extract certain columns
def get_card_parts(cards: pd.DataFrame):
    card_parts_start = cards.loc[cards["all_parts"].notna() ,["id", "all_parts"]]
    parts_explode = card_parts_start.explode("all_parts")
    nested_parts = pd.DataFrame.from_dict(parts_explode["all_parts"])
    nested_parts["object"] = nested_parts["all_parts"].str["object"]
    nested_parts["component"] = nested_parts["all_parts"].str["component"]
    nested_parts["id"] = nested_parts["all_parts"].str["id"]

    card_parts = pd.merge(card_parts_start, nested_parts, left_index=True, right_index=True)
    card_parts = card_parts.rename(columns={"id_x":"card_id", "id_y":"related_card"})
    card_parts = card_parts.drop(["all_parts_x", "all_parts_y"], axis=1)

    return card_parts

if __name__ == "__main__":
    new_cards = pd.read_json("test_data.json" ,orient="records")
    # df = get_card_parts(new_cards)



card_types_lookup = new_cards["type_line"].str.split(" ").explode().drop_duplicates()
# this then needs to be transformed into a list of the "id" column of each json object