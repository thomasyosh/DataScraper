from sqlalchemy.orm import sessionmaker, scoped_session, declarative_base
from sqlalchemy import create_engine, MetaData, Table, select, update, func, text
import json
from sqlalchemy import Column, String, Boolean, DateTime, Integer, JSON, Double, Date
from sqlalchemy_guid import GUID
import uuid
import datetime
import pandas as pd
import sqlite3


Base = declarative_base()

DATABASE_URL = "results.db"
engine = create_engine(f"sqlite:///{DATABASE_URL}?charset=utf8mb4",
                       echo = False,
                       json_serializer = lambda obj: 
                           json.dumps(obj,
                                      ensure_ascii = False)\
                                          .encode("utf-8"))

session = scoped_session(
    sessionmaker(
        autoflush = True,
        autocommit = False,
        bind = engine
    )
)

class AddressMaster(Base):
    __tablename__ = "address_master"
    id = Column(GUID, primary_key = True, default = uuid.uuid4)
    address = Column(String)
    easting = Column(Double)
    northing = Column(Double)
    csuid = Column(String)
    is_chinese = Column(Boolean, primary_key = True, default = False)
    create_date = Column(DateTime(timezone = True), default = datetime.datetime.now)

class PoiMaster(Base):
    __tablename__ = "poi_master"
    id = Column(String, primary_key = True)
    name = Column(String)
    address = Column(String)
    floor = Column(String)
    unit = Column(String)
    csuid = Column(String)
    telno = Column(Integer)
    faxno = Column(Integer)
    website = Column(String)
    status = Column(String)
    mdate = Column(DateTime)
    poi_type = Column(String)
    easting = Column(Double)
    northing = Column(Double)
    is_chinese = Column(Boolean, primary_key = True, default = False)
    create_date = Column(DateTime(timezone = True), server_default = func.now())

class AddressResult(Base):
    __tablename__ = "address_results"
    id = Column(String, primary_key = True, default = uuid.uuid4)
    address = Column(String)
    endpoint = Column(String, primary_key = True)
    result = Column(JSON, nullable = True)
    response_code = Column(Integer)
    is_chinese = Column(Boolean, primary_key = True)
    create_date = Column(DateTime(timezone = True), default = datetime.datetime.now)
    update_date = Column(DateTime(timezone = True), onupdate = datetime.datetime.now)

Base.metadata.create_all(engine)

def readAllAddressMaster() -> list[PoiMaster]:
    result = session.query(PoiMaster).all()
    session.close()
    return result

def readAddressMasterById(id : str) -> list[AddressMaster]:
    result = session.query(AddressMaster)\
        .where(AddressResult.id == id)\
            .all()
    session.close()
    return result

def getRemainingMaster()->list[PoiMaster]:
    result = session.query(PoiMaster)\
        .join(AddressResult,(PoiMaster.id == AddressResult.id),isouter=True)\
            .filter(AddressResult.id == None)\
                .all()
    session.close()
    return result

def getAddressDetailById(id : str) -> list[AddressResult]:
    result = session.query(AddressResult.id,
                           AddressResult.result,
                           AddressResult.address,
                           AddressResult.endpoint,
                           AddressMaster.easting,
                           AddressMaster.northing,
                           AddressResult.is_chinese,
                           AddressMaster.csuid)\
        .join(AddressMaster, (AddressResult.id == AddressMaster.id) &
            (AddressResult.is_chinese == AddressMaster.is_chinese))\
                  .where(AddressResult.id == id)\
                .all()
    return result

def caseDetailWithCountLessThan(recordsPerId : int) -> list[PoiMaster]:
    result = session.scalars(select(PoiMaster.id)\
        .join(AddressResult, (AddressResult.id == PoiMaster.id) &
            (AddressResult.is_chinese == PoiMaster.is_chinese))\
                .group_by(AddressResult.id)\
                    .having(func.count(AddressResult.id) != recordsPerId))\
                .all()
    session.close()
    return result

def getPoiMasterByIds(ids : list[str]) -> list[PoiMaster]:
    batches = []
    batchSize = 20000
    for i in range(0, len(ids), batchSize):
        chunk = ids[i:i + batchSize]
        result = session.query(PoiMaster)\
            .filter(PoiMaster.id.in_(chunk))\
                .all()
        session.close()
        batches.extend(result)
    return batches

def getCaseWithoutResult() -> list[AddressResult]:
    result = session.query(AddressResult)\
        .where(AddressResult.response_code != 200)\
            .all()
    session.close()
    return result

def getDistinctAddressId() -> list[str]:
    result = session.scalars(select(AddressMaster.id)\
        .order_by(AddressMaster.create_date)\
            .distinct()).all()
    session.close()
    return result


def updateAddressResultByEndpoints(endpoint : str,
                                   newEndpoint : str) -> None:

    result = session.query(AddressResult)\
        .where(AddressResult.endpoint == endpoint)\
            .all()

    for item in result:
        item.endpoint = newEndpoint

    session.commit()
    session.close()

def insertToMaster(dataframe: pd.DataFrame) -> None:
    dataframe.to_sql(name="poi_master", if_exists="append", con=engine, index=False)

def insertExcelToMaster(dateframe : pd.DataFrame) -> None:
    for index, row in dateframe.iterrows():
        chineseAddress = AddressMaster(address = row["name_tc"],
                                       easting = row["lat"],
                                       northing = row["long"],
                                       csuid = row["stop"],
                                       is_chinese = True,
                                       )
        session.add(chineseAddress)
        session.flush()
        session.refresh(chineseAddress)
        englishAddress = AddressMaster(id = chineseAddress.id,
                                       address = row["name_en"],
                                       easting = row["lat"],
                                       northing = row["long"],
                                       csuid = row["stop"],
                                       is_chinese = False,
                                       )
        session.add(englishAddress)
        session.commit()
        session.close()

def insertExcelToPoiMaster(dataframe : pd.DataFrame) -> None:
    english_df = dataframe[[
        "POIID", 
        # "ENAME", 
        "EADDRESS", 
        # "EFLOOR",
        # "EUNIT",
        "buildingcsuid",
        # "TELNO",
        # "FAXNO", 
        # "WEBSITE",
        # "STATUS",
        # "MDATE",
        # "TYPE",
        "EASTING",
        "NORTHING"
        ]].copy()

    chinese_df = dataframe[[
        "POIID",
        # "CNAME",
        "CADDRESS",
        # "CFLOOR",
        # "CUNIT",
        "buildingcsuid",
        # "TELNO",
        # "FAXNO", 
        # "WEBSITE",
        # "STATUS",
        # "MDATE",
        # "TYPE",
        "EASTING",
        "NORTHING"
    ]].copy()
    
    english_df = english_df.rename(columns={"ENAME": "NAME", "EADDRESS": "ADDRESS", "EFLOOR": "FLOOR", "EUNIT": "UNIT"})
    chinese_df = chinese_df.rename(columns={"CNAME": "NAME", "CADDRESS": "ADDRESS", "CFLOOR": "FLOOR", "CUNIT": "UNIT"})

    english_df['is_chinese'] = False
    chinese_df['is_chinese'] = True
    english_df = english_df.drop_duplicates(subset=["ADDRESS"], keep="first")
    chinese_df = chinese_df.drop_duplicates(subset=["ADDRESS"], keep="first")
    english_df = english_df[english_df["ADDRESS"].notna()]
    chinese_df = chinese_df[chinese_df["ADDRESS"].notna()]
    result_df = pd.concat([english_df, chinese_df], ignore_index=True)
    # result_df['POIID'] = pd.Categorical(result_df['POIID'], categories=english_df['POIID'], ordered=True)
    # result_df = result_df.sort_values('POIID').reset_index(drop=True)

    result_df.columns = map(str.lower, result_df.columns)
    result_df = result_df.rename(columns={"poiid":"id","type":"poi_type", "buildingcsuid": "csuid"})
    # result_df = result_df.drop_duplicates(subset=["address"], keep="first")
    result_df.to_sql(name="poi_master", if_exists="append", con=engine, index=False)
    
    
def createResultTable()->None:
    with open("sql/chin_result.sql", "r") as sql_file:
        chinResultSql = sql_file.read()
    
    with open("sql/eng_result.sql", "r") as sql_file:
        engResultSql = sql_file.read()
        
    with open("sql/chin_api_performance.sql", "r") as sql_file:
        chinApiPerformanceSql = sql_file.read()
    
    with open("sql/eng_api_performance.sql", "r") as sql_file:
        engApiPerformanceSql = sql_file.read()
        
    with open("sql/chin_summary.sql", "r") as sql_file:
        chinSummarySql = sql_file.read()

    with open("sql/eng_summary.sql", "r") as sql_file:
        engSummarySql = sql_file.read()

    db = sqlite3.connect(DATABASE_URL)
    cursor = db.cursor()
    cursor.executescript(chinResultSql)
    cursor.executescript(engResultSql)
    cursor.executescript(chinApiPerformanceSql)
    cursor.executescript(engApiPerformanceSql)
    cursor.executescript(chinSummarySql)
    cursor.executescript(engSummarySql)
    db.commit()
    db.close()