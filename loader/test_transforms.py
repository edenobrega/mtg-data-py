import unittest
import main as ETL
import mtg_transform as mt
import datetime as dt
import pandas as pd
import logging
import numpy as np
from dotenv import load_dotenv
from os import mkdir, path, getenv, remove
from pandas.testing import assert_frame_equal

# https://docs.python.org/3/library/unittest.html
class TestTransforms(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ETL.BULK_NAME = getenv("TCGCT_TEST_BULK_NAME")
        ETL.LOG_LEVEL = int(getenv('TCGCT_TEST_LOG_LEVEL'))
        ETL.LOAD_STRAT = str(getenv("TCGCT_TEST_LOAD_STRAT"))
        ETL.log = logging.getLogger(__name__)
        _cards, _sets, _sets_data = ETL.extract()
        
        ETL.BULK_NAME = getenv("TCGCT_TEST_FACES_BULK_NAME")
        _card_faces, _discard, _discard = ETL.extract()
        
        cls.cards: pd.DataFrame = _cards
        cls.sets: pd.DataFrame = _sets

        cls.card_faces: pd.DataFrame = _card_faces

        del _discard
    
    def test_prepare(self):
        prepared = mt.prepare_cards(self.cards)
        columns = ["name", "mana_cost", "oracle_text", "flavor_text", "artist", "collector_number",
                            "power", "toughness", "set", "id", "cmc", "oracle_id", "rarity", "layout", "card_faces", "image_uris", "loyalty", "type_line", "all_parts"]
        self.assertEqual(columns, list(prepared.columns))
    
    def test_get_cards(self):
        # There isnt actually any transformations really going on here, as long as :
        #   1. All columns are present (as there are times they are not)
        #   2. All image columns are checked

        # TODO: i feel like there should be some sort of dependancy check here
        prepared_cards = mt.prepare_cards(self.cards)
        test_data = mt.get_cards(prepared_cards)

        columns = ['name', 'mana_cost', 'oracle_text', 'flavor_text', 'artist',
                    'collector_number', 'power', 'toughness', 'set', 'id', 'cmc',
                    'oracle_id', 'rarity', 'layout', 'loyalty', 'normal']

        self.assertEqual(columns, list(test_data.columns))

        test_images = [
            None,
            None,
            None,
            None,
            "https://cards.scryfall.io/normal/front/0/0/0000579f-7b35-4ed3-b44c-db2a538066fe.jpg?1562894979",
            None,
            "https://cards.scryfall.io/normal/front/9/0/90f17b85-a866-48e8-aae0-55330109550e.jpg?1562488879",
            "https://cards.scryfall.io/normal/front/d/b/db7c07b2-02b2-4e62-bf1b-4848e06eec28.jpg?1712355592"
        ]

        test_series = pd.Series(data=test_images)

        self.assertTrue(test_data["normal"].equals(test_series))

    def test_get_card_faces(self):
        test_data = mt.get_card_faces(self.card_faces)

        test_faces = {
            'id':[
                "3e3f0bcd-0796-494d-bf51-94b33c1671e9",
                "3e3f0bcd-0796-494d-bf51-94b33c1671e9",
                "0b61d772-2d8b-4acf-9dd2-b2e8b03538c8",
                "0b61d772-2d8b-4acf-9dd2-b2e8b03538c8"
            ],
            'object':[
                "card_face",
                "card_face",
                "card_face",
                "card_face"
            ],
            'name':[
                "Propaganda",
                "Propaganda",
                "Erayo, Soratami Ascendant",
                "Erayo's Essence"
            ],
            'mana_cost':[
                "{2}{U}",
                "{2}{U}",
                "{1}{U}",
                None
            ],
            'oracle_text':[
                "Creatures can't attack you unless their controller pays {2} for each creature they control that's attacking you.",
                "Creatures can't attack you unless their controller pays {2} for each creature they control that's attacking you.",
                "Flying\nWhenever the fourth spell of a turn is cast, flip Erayo, Soratami Ascendant.",
                "Whenever an opponent casts their first spell each turn, counter that spell."
            ],
            'cmc':[
                3.0,
                3.0,
                None,
                None
            ],
            'flavor_text':[
                "\"Obedience is the true path to happiness.\"\n—Dolorus, magister equitum",
                "\"Why rebel? The system can be changed from within.\"\n—Dolorus, magister equitum",
                None,
                None
            ],
            'loyalty':[
                None,
                None,
                None,
                None
            ],
            'oracle_id':[
                "ea9709b6-4c37-4d5a-b04d-cd4c42e4f9dd",
                "ea9709b6-4c37-4d5a-b04d-cd4c42e4f9dd",
                "d83bb32d-f409-421a-9d93-bd7ea3240b47",
                "d83bb32d-f409-421a-9d93-bd7ea3240b47"
            ],
            'power':[
                None,
                None,
                "1",
                None
            ],
            'toughness':[
                None,
                None,
                "1",
                None
            ],
            'image':[
                "https://cards.scryfall.io/normal/front/3/e/3e3f0bcd-0796-494d-bf51-94b33c1671e9.jpg?1670031727",
                "https://cards.scryfall.io/normal/back/3/e/3e3f0bcd-0796-494d-bf51-94b33c1671e9.jpg?1670031727",
                "https://cards.scryfall.io/normal/front/0/b/0b61d772-2d8b-4acf-9dd2-b2e8b03538c8.jpg?1610664131",
                "https://cards.scryfall.io/normal/front/0/b/0b61d772-2d8b-4acf-9dd2-b2e8b03538c8.jpg?1610664131"
            ]
        }

        compare_frame: pd.DataFrame = pd.DataFrame(data=test_faces) 

        # TODO: I absolutely hate this, i need to handle np.nan better, i feel like doing transformations in a unittest is horrible practise
        #           the problem being that comparing the frames always fails as np.nan != np.nan etc etc, perhaps in main code need to just
        #           somehow replace all nan na etc with None
        test_data.replace([np.nan, np.NaN], [None, None], inplace=True)
        del test_data["index"]

        assert_frame_equal(test_data, compare_frame, check_dtype=False)

    def test_get_card_parts(self):
        test_data = mt.get_card_parts(self.cards)

        raw_data = {
            "card_id":[
                "b37aa12c-a6b3-4cf8-b5a4-0a999ff12d02",
                "b37aa12c-a6b3-4cf8-b5a4-0a999ff12d02",
                "b37aa12c-a6b3-4cf8-b5a4-0a999ff12d02",
                "b37aa12c-a6b3-4cf8-b5a4-0a999ff12d02",
                "ba09360a-067e-48a5-bdc5-a19fd066a785",
                "ba09360a-067e-48a5-bdc5-a19fd066a785",
                "db7c07b2-02b2-4e62-bf1b-4848e06eec28",
                "db7c07b2-02b2-4e62-bf1b-4848e06eec28"
            ],
            "object":[
                "related_card",
                "related_card",
                "related_card",
                "related_card",
                "related_card",
                "related_card",
                "related_card",
                "related_card"
            ],
            "component":[
                "combo_piece",
                "combo_piece",
                "token",
                "combo_piece",
                "combo_piece",
                "token",
                "combo_piece",
                "token"
            ],
            "related_card":[
                "b37aa12c-a6b3-4cf8-b5a4-0a999ff12d02",
                "bb0686c3-a44a-449d-ab12-eeb9c0c25489",
                "0a5ac360-dc47-4bc5-a4cc-ff223abc3ffc",
                "4825c59d-9e27-4a12-ba84-642b8540e573",
                "786156ce-544c-4aa4-8381-756042d0bcda",
                "94a50acd-ac2d-47bf-b331-0bcf5edd9c75",
                "37c1ad56-cf6e-4717-a56e-feeb7339b8c3",
                "74c7a0bd-6011-495a-b56c-8fa707dd7f12"
            ]
        }

        parts_compare = pd.DataFrame(data=raw_data)
        self.assertTrue(test_data.reset_index(drop=True).equals(parts_compare.reset_index(drop=True)))

    def test_get_type_line_data(self):
        # alpha: unique types
        # bravo: types to cards
        test_unique_types, test_types_to_cards = mt.get_type_line_data(self.cards)
        
        test_compare = {
            "id": [
                "0b61d772-2d8b-4acf-9dd2-b2e8b03538c8",
                "0b61d772-2d8b-4acf-9dd2-b2e8b03538c8",
                "0b61d772-2d8b-4acf-9dd2-b2e8b03538c8",
                "0b61d772-2d8b-4acf-9dd2-b2e8b03538c8",
                "0b61d772-2d8b-4acf-9dd2-b2e8b03538c8",
                "0b61d772-2d8b-4acf-9dd2-b2e8b03538c8",
                "0b61d772-2d8b-4acf-9dd2-b2e8b03538c8",
                "0b61d772-2d8b-4acf-9dd2-b2e8b03538c8",
                "b37aa12c-a6b3-4cf8-b5a4-0a999ff12d02",
                "b37aa12c-a6b3-4cf8-b5a4-0a999ff12d02",
                "b37aa12c-a6b3-4cf8-b5a4-0a999ff12d02",
                "b37aa12c-a6b3-4cf8-b5a4-0a999ff12d02",
                "b37aa12c-a6b3-4cf8-b5a4-0a999ff12d02",
                "b37aa12c-a6b3-4cf8-b5a4-0a999ff12d02",
                "b37aa12c-a6b3-4cf8-b5a4-0a999ff12d02",
                "b37aa12c-a6b3-4cf8-b5a4-0a999ff12d02",
                "b37aa12c-a6b3-4cf8-b5a4-0a999ff12d02",
                "ba09360a-067e-48a5-bdc5-a19fd066a785",
                "ba09360a-067e-48a5-bdc5-a19fd066a785",
                "ba09360a-067e-48a5-bdc5-a19fd066a785",
                "ba09360a-067e-48a5-bdc5-a19fd066a785",
                "ba09360a-067e-48a5-bdc5-a19fd066a785",
                "ba09360a-067e-48a5-bdc5-a19fd066a785",
                "ba09360a-067e-48a5-bdc5-a19fd066a785",
                "0000579f-7b35-4ed3-b44c-db2a538066fe",
                "0000579f-7b35-4ed3-b44c-db2a538066fe",
                "0000579f-7b35-4ed3-b44c-db2a538066fe",
                "00320106-ce51-46a9-b0f9-79b3baf4e505",
                "00320106-ce51-46a9-b0f9-79b3baf4e505",
                "00320106-ce51-46a9-b0f9-79b3baf4e505",
                "90f17b85-a866-48e8-aae0-55330109550e",
                "90f17b85-a866-48e8-aae0-55330109550e",
                "90f17b85-a866-48e8-aae0-55330109550e",
                "90f17b85-a866-48e8-aae0-55330109550e",
                "db7c07b2-02b2-4e62-bf1b-4848e06eec28",
                "db7c07b2-02b2-4e62-bf1b-4848e06eec28",
                "db7c07b2-02b2-4e62-bf1b-4848e06eec28",
                "db7c07b2-02b2-4e62-bf1b-4848e06eec28",
                "db7c07b2-02b2-4e62-bf1b-4848e06eec28",
                "3e3f0bcd-0796-494d-bf51-94b33c1671e9",
                "3e3f0bcd-0796-494d-bf51-94b33c1671e9"
            ],
            "type_name": [
                "Legendary",
                "Creature",
                "—",
                "Moonfolk",
                "Monk",
                "//",
                "Legendary",
                "Enchantment",
                "Legendary",
                "Planeswalker",
                "—",
                "Arlinn",
                "//",
                "Legendary",
                "Planeswalker",
                "—",
                "Arlinn",
                "Legendary",
                "Creature",
                "—",
                "Human",
                "Warlock",
                "//",
                "Sorcery",
                "Creature",
                "—",
                "Sliver",
                "Instant",
                "//",
                "Sorcery",
                "Creature",
                "—",
                "Human",
                "Child",
                "Legendary",
                "Creature",
                "—",
                "Human",
                "Warlock",
                "Enchantment",
                "Enchantment"

            ]
        }
        compare_frame = pd.DataFrame(data=test_compare)
        self.assertTrue(test_types_to_cards.reset_index(drop=True).equals(compare_frame))

        del compare_frame, test_compare
        test_compare = {
            "type_line": [
                "Legendary",
                "Creature",
                "—",
                "Moonfolk",
                "Monk",
                "//",
                "Enchantment",
                "Planeswalker",
                "Arlinn",
                "Human",
                "Warlock",
                "Sorcery",
                "Sliver",
                "Instant",
                "Child",
                "Enchantment"
            ]
        }
        
        
        compare_frame = pd.DataFrame(data=test_compare)

        self.assertTrue(test_unique_types.reset_index(drop=True).equals(compare_frame))

    def test_get_rarities(self):
        test_data = mt.get_rarities(self.cards)

        test_rarities = [
            "rare",
            "mythic",
            "uncommon",
            "common"
        ]

        test_series = pd.Series(data=test_rarities)
        self.assertTrue(test_data.reset_index(drop=True).equals(test_series))

    def test_get_layouts(self):
        test_data = mt.get_layouts(self.cards)

        test_layouts = [
            "flip",
            "transform",
            "modal_dfc",
            "reversible_card",
            "normal",
            "split"
        ]

        test_series = pd.Series(data=test_layouts)
        self.assertTrue(test_data.equals(test_series))
