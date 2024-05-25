# mtg-loader-py
mtg scryfall loader using pandas

Gets data from [scryfall API](https://scryfall.com/docs/api)

# env settings
## Main
- TCGCT_LOG_LEVEL
    - Set the logging level to be used
    - https://docs.python.org/3/library/logging.html#logging-levels
- Database settings
    - TCGCT_DB_PROTECTED
        - Whether or not the DB requires a password and username
    - TCGCT_DB_USERNAME
    - TCGCT_DB_PASSWORD
    - TCGCT_DB_DRIVER
    - TCGCT_DB_NAME
    - TCGCT_DB_LOCATION
- TCGCT_BULK_NAME
    - Name of the DB
- TCGCT_CONNECTION_STRING
    - No longer in use
    - TODO: Perhaps have as a second option for different connector types
- TCGCT_BULK_NAME
    - Location and name of the file to store bulk .json files
- TCGCT_LOAD_STRAT="LOCAL"
    - Defines how the raw data is acquired
    - DOWNLOAD
        - Downloads the latest bulk .json from the scryfall API, and then saves it to the `TCGCT_BULK_NAME` location
    - LOCAL
        - Uses existing file in the provided `TCGCT_BULK_NAME` directory
    - API
        - Uses the scryfall API to get all Sets, and then loop through all sets and check if our provided DB (`TCGCT_BULK_NAME`) data matches the API

Example :
```
TCGCT_LOG_LEVEL=10
TCGCT_DB_NAME="tcgct-dev"
TCGCT_DB_LOCATION="DESKTOP-UPNS42E\SQLEXPRESS"
TCGCT_DB_DRIVER="ODBC Driver 17 for SQL Server"
TCGCT_DB_USERNAME="SA"
TCGCT_DB_PASSWORD="yourStrong(!)Password"
TCGCT_DB_PROTECTED="False"
TCGCT_BULK_NAME="data/bulk_data.json"
TCGCT_LOAD_STRAT="DOWNLOAD"
```




## Testing
TCGCT_TEST_LOG_LEVEL=10
TCGCT_TEST_BULK_NAME="Testing/test_data.json"
TCGCT_TEST_LOAD_STRAT="LOCAL"

# Other stuff
![](docs_assets/dbs.png)
