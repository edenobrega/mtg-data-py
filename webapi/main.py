import sqlalchemy as sa
import logging as lo
import datetime as dt
import jwt
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from pydantic import BaseModel
from os import getenv, mkdir, path
from typing import Annotated
from jwt.exceptions import InvalidTokenError

APP_SETTINGS = {

}

SECRET_KEY = "f72d92297881a44e0f56cbdd8b32a7bde4a79ae33c8c1e23ded845100605755f"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

def lifespan(app: FastAPI):
    load_dotenv()
    try:
        LOG_LEVEL = int(getenv('LOG_LEVEL'))
    except:
        LOG_LEVEL = 10

    try:
        APP_SETTINGS["USERNAME_MINIMUM_LENGTH"] = int(getenv('USERNAME_MINIMUM_LENGTH'))
    except:
        APP_SETTINGS["USERNAME_MINIMUM_LENGTH"] = 6

    try:
        APP_SETTINGS["PASSWORD_MINIMUM_LENGTH"] = int(getenv('PASSWORD_MINIMUM_LENGTH'))
    except:
        APP_SETTINGS["PASSWORD_MINIMUM_LENGTH"] = 6

    try:
        APP_SETTINGS["DB_NAME"] = getenv("DB_NAME")
        APP_SETTINGS["DB_LOCATION"] = getenv("DB_LOCATION")
        APP_SETTINGS["DB_DRIVER"] = getenv("DB_DRIVER")
        APP_SETTINGS["DB_USERNAME"] = getenv("DB_USERNAME")
        APP_SETTINGS["DB_PASSWORD"] = getenv("DB_PASSWORD")
    except Exception as ex:
        print("something went wrong when getting env : "+str(ex))
        raise

    if not path.isdir('logs'):
        mkdir('logs/')

    app.logger = lo.getLogger(__name__)

    lo.basicConfig(level=lo._levelToName[LOG_LEVEL],
                    filename='logs/'+str(dt.datetime.today().date())+'.txt',
                    format='%(asctime)s | %(levelname)s | Line:%(lineno)s | %(message)s',
                    filemode='a'
                )
    yield

app = FastAPI(lifespan=lifespan)

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

#region Classes
class Token(BaseModel):
    access_token: str
    token_type: str

class CollectionUpdateItem(BaseModel):
    # Set Code, Card ID, Change Amount
    ids: list[tuple[str, str, int]]

class CreateUserItem(BaseModel):
    username: str
    password: str

class User(BaseModel):
    id: int
    uid: str
    username: str
    password: str
#endregion

#region Helpers
def create_connection(db_name: str, db_location: str, db_driver: str, db_username: str, db_password: str) -> sa.Engine:
    connection_url = sa.URL.create(
        "mssql+pyodbc",
        username=db_username,
        password=db_password,
        host=db_location,
        database=db_name,
        query={"driver": db_driver},
    )
    engine = sa.create_engine(connection_url)
    app.logger.debug("testing db connection")
    try:
        with engine.connect() as conn:
            conn.execute(sa.text("SELECT 1"))
    except:
        app.logger.fatal("failed to create connection")
        return None
    app.logger.debug("connection success")
    return engine

def get_user(username: str) -> User | None:
    engine: sa.Engine = create_connection(APP_SETTINGS["DB_NAME"], APP_SETTINGS["DB_LOCATION"], APP_SETTINGS["DB_DRIVER"], APP_SETTINGS["DB_USERNAME"], APP_SETTINGS["DB_PASSWORD"])
    with engine.connect() as conn:
        sql = sa.text("SELECT [ID], [UID], [Username], [Password] FROM [Account].[User] WHERE [Username] = :param_username")
        sql = sql.bindparams(param_username=username)
        ret = conn.execute(sql).one_or_none()
        if ret is None:
            return None
        user = User(id=ret[0], uid=ret[1], username=ret[2], password=ret[3])
    return user

#endregion

#region auth
async def check_valid_access_token(token: Annotated[str, Depends(oauth2_scheme)]):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub::username")
        if username is None:
            raise credentials_exception
    except InvalidTokenError:
        raise credentials_exception
    user = get_user(username)
    if user is None:
        raise credentials_exception
    return user

def get_password_hash(password) :
    hashed_password = generate_password_hash(password=password,salt_length=16)
    return hashed_password

def create_access_token(data: dict, expires_delta: timedelta | None = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        expire = datetime.now(timezone.utc) + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

@app.post("/token")
async def login(form_data: Annotated[OAuth2PasswordRequestForm, Depends()]) -> Token:
    user = get_user(form_data.username)

    if user is None:
        raise HTTPException(status_code=400, detail="Incorrect username or password")
    if check_password_hash(user.password, form_data.password) == False:
        raise HTTPException(status_code=400, detail="Incorrect username or password")

    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)

    access_token = create_access_token(
        data={"sub::username": user.username}, expires_delta=access_token_expires
    )

    return Token(access_token=access_token, token_type="bearer")
#endregion

@app.get("/")
def read_root():
    engine:sa.Engine = create_connection(APP_SETTINGS["DB_NAME"], APP_SETTINGS["DB_LOCATION"], APP_SETTINGS["DB_DRIVER"], APP_SETTINGS["DB_USERNAME"], APP_SETTINGS["DB_PASSWORD"])
    with engine.connect() as conn:
        sql = sa.text("SELECT @@VERSION")
        data = conn.execute(sql).one_or_none()
        return {"db_test": data[0]}

@app.get("/Collection")
def read_item(token: Annotated[User, Depends(oauth2_scheme)]):
    payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    print(payload)
    print(token)
    return {
        "one": payload.get("sub::username")
    }

@app.patch("/Collection/Update")
def update_item(item: CollectionUpdateItem, token: Annotated[User, Depends(check_valid_access_token)]):
    print(token)
    print(item)

    engine:sa.Engine = create_connection(APP_SETTINGS["DB_NAME"], APP_SETTINGS["DB_LOCATION"], APP_SETTINGS["DB_DRIVER"], APP_SETTINGS["DB_USERNAME"], APP_SETTINGS["DB_PASSWORD"])
    with engine.connect() as conn:
        conn.execute(sa.text("[Collection].[UpdateMTGCollection] ?, ?"), [token.id, item])


    return {"success": True}

@app.post("/User/Create")
def create_user(item: CreateUserItem):
    if len(item.username) < APP_SETTINGS["USERNAME_MINIMUM_LENGTH"]:
        raise HTTPException(status_code=400, detail="Username did not comply with the minimum length of "+str(APP_SETTINGS["USERNAME_MINIMUM_LENGTH"]))
    if len(item.password) < APP_SETTINGS["PASSWORD_MINIMUM_LENGTH"]:
        raise HTTPException(status_code=400, detail="Username did not comply with the minimum length of "+str(APP_SETTINGS["PASSWORD_MINIMUM_LENGTH"]))
    if get_user(item.username) is not None:
        raise HTTPException(status_code=400, detail="Username is taken")    

    success: bool = False

    password_hash = get_password_hash(item.password)
    engine:sa.Engine = create_connection(APP_SETTINGS["DB_NAME"], APP_SETTINGS["DB_LOCATION"], APP_SETTINGS["DB_DRIVER"], APP_SETTINGS["DB_USERNAME"], APP_SETTINGS["DB_PASSWORD"])
    try:
        with engine.connect() as conn:
            sql = sa.text("INSERT INTO [Account].[User]([Username], [Password]) VALUES(:param_username, :param_password)")
            sql = sql.bindparams(param_username=item.username, param_password=password_hash)
            conn.execute(sql)
            conn.commit()
        success = True
    except:
        success = False
    return {"success": success}
