import unittest
import main as ETL
import mtg_transform as mt
import datetime as dt
import pandas as pd
import logging
from dotenv import load_dotenv
from os import mkdir, path, getenv, remove


# https://docs.python.org/3/library/unittest.html
class TestLoader(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ETL.BULK_NAME = getenv("TCGCT_TEST_BULK_NAME")
        ETL.LOG_LEVEL = int(getenv('TCGCT_TEST_LOG_LEVEL'))
        ETL.LOAD_STRAT = str(getenv("TCGCT_TEST_LOAD_STRAT"))
        ETL.log = logging.getLogger(__name__)
        _cards, _sets = ETL.extract()
        cls.cards: pd.DataFrame = _cards
        cls.sets: pd.DataFrame = _sets
    
    def test_extract(self):
        self.assertEqual(self.cards.shape[0], 7)
