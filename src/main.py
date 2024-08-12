import asyncio
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.dialects.postgresql import insert
import json
import pandas as pd
import datetime
import re
from hk1980 import LatLon, HK80
from geopy.distance import geodesic
from database import *
from api import *
import requests
import python_calamine
import os

async def fetchData(engine, endpoints, client, rows):
    tasks = []
    
    async def handleRow(row):
        async def insertToDb(endpoint):
            data, response_code = await endpoint.fetchOne(client, row)
            update_values = {
                        'result': data,
                        'response_code': response_code,
                        'update_date': datetime.datetime.now()
                    }
        
            stmt = insert(AddressResult).values(
                id=row.id,
                address=row.address,
                endpoint=endpoint.base_url,
                result=data,
                response_code=response_code,
                is_chinese=row.is_chinese,
                create_date=datetime.datetime.now(),
            ).on_conflict_do_update(
                index_elements=['id', 'endpoint', 'is_chinese'],
                set_=update_values
            )
        
            session.execute(stmt)
            session.commit()

        await asyncio.gather(*[insertToDb(endpoint) for endpoint in endpoints])

    for row in rows:
        tasks.append(asyncio.ensure_future(handleRow(row)))
    await asyncio.gather(*tasks)

async def get_rows_from_db():
    return readAllAddressMaster()

async def main(addressMaster : list[AddressMaster], endpoints : list[Endpoint])-> None:
    batches = []
    batchSize = 2000
    for i in range(0, len(addressMaster), batchSize):
        batch = addressMaster[i:i+batchSize]
        batches.append(batch)
        
    connector = aiohttp.TCPConnector()
    async with aiohttp.ClientSession(connector = connector) as client:
        for batch in batches:
            await fetchData(engine, endpoints, client, batch)
            logging.info(f"sleep for one minutes")
            await asyncio.sleep(360)

async def post_request():
    async with aiohttp.ClientSession() as session:
        response = await session.post(url = "http://10.77.242.157:8888/query_debug",
                                      json = {"address": ["apm"]},
                                      headers = {"Content-Type": "application/json"},
                                      )
        print(await response.json())

def compareCoorDistance(inputNorthing : float,
                        inputEasting : float,
                        apiNorthing : float, 
                        apiEasting : float) -> bool:
    apiCoor = HK80(northing=apiNorthing, easting=apiEasting).to_wgs84()
    apiWgsCoor = (apiCoor.latitude, apiCoor.longitude)
    inputCoor = HK80(northing=inputNorthing, easting=inputEasting).to_wgs84()
    inputWgsCoor = (inputCoor.latitude, inputCoor.longitude)
    distance = geodesic(apiWgsCoor,inputWgsCoor).meters
    return True if distance <= 50 else False

def getDataframeByAddressId(addressId : str,
                            numberOfRecord : int) ->pd.DataFrame:

    addressSearchDataFrame = []
    geoDataFrame = []
    alsDataFrame = []
    data = getAddressDetailById(addressId)
    for item in data:
        if item.endpoint == "http://10.77.242.157:8888/query_debug":
            for record in item.result["data"][item.address][:numberOfRecord]:
                addressSearchDataFrame.append(
                    {
                        "addressId" : item.id,
                        "isChineseAddress" : item.is_chinese,
                        "inputAddress" : item.address,
                        "inputEasting" : item.easting,
                        "inputNorthing" : item.northing,
                        "inputCSUID" : item.csuid,
                        "addressSearchChinAddress" : record["name_zh"],
                        "addressSearchEngAddress" : record["name_en"],
                        "ad_searchIndex" : record["index"],
                        "ad_csuid" : record["building_csuid"],
                        "ad_easting" : record["easting"],
                        "ad_northing" : record["northing"],
                        "ad_withinMeter" : compareCoorDistance(item.northing,
                                                            item.easting,
                                                            record["northing"],
                                                            record["easting"],
                                                            )
                        })
        elif item.endpoint == "https://geodata.gov.hk/gs/api/v1.0.0/locationSearch":
            for record in item.result[:numberOfRecord]:
                re.sub("[^\x20-\x7E]", "", record['addressEN'])
                geoDataFrame.append(
                    {
                        "addressId" : item.id,
                        "isChineseAddress" : item.is_chinese,
                        "inputAddress" : item.address,
                        "inputEasting" : item.easting,
                        "inputNorthing" : item.northing,
                        "inputCSUID" : item.csuid,
                        "geoDataChinAddress" : f'{record["addressZH"]}{record["nameZH"]}',
                        "geoDataEngAddress" : f'{record["nameEN"]}, {record["addressEN"]}',
                        "gd_easting" : record["x"],
                        "gd_northing" : record["y"],
                        "gd_withinMeter" : compareCoorDistance(item.northing,
                                                            item.easting,
                                                            record["y"],
                                                            record["x"],
                                                            )
                        }
                )
        else:
            pass
        
    df = pd.DataFrame(addressSearchDataFrame)
    df2 = pd.DataFrame(geoDataFrame)
    print(df)
    print(df2)
    
    # df.reset_index(drop=True, inplace=True)
    # df2.reset_index(drop=True, inplace=True)
    # df2.columns = [f"Extra_{col}" for col in df2.columns]
    duplicated_columns = df.columns.intersection(df2.columns)
    # df2 = df2.drop(columns=duplicated_columns)
    df3 = pd.merge(df, df2, on=["addressId", "isChineseAddress", "inputAddress", "inputCSUID", "inputEasting", "inputNorthing"], how="inner")
    # df3 = pd.merge(df, df2, on="inputAddress", how="outer")
    return df3

def getAllDataframe(numberOfRecord : int,
                    addressId : list[str]) ->pd.DataFrame:
    for id in addressId:
        df = getDataframeByAddressId(id, 5)
        for column in df.columns:
            if column == 'addressId'\
                or column == 'isChineseAddress'\
                    or column == "inputAddress"\
                        or column == "inputEasting"\
                            or column == "inputNorthing":
                df[column] = df[column].ffill()
            else:
                df[column] = df[column].fillna('Not applicable')
        print(df)
    
    chinAddr = df[(df["isChineseAddress"] == True)]
    engAddr = df[(df["isChineseAddress"] == False)]
    pass

def getDummyData() ->json:
    response = requests\
        .get("https://data.etabus.gov.hk/v1/transport/kmb/stop")
    if response.status_code == 200:
        return response.json()

        
if __name__ == "__main__":
    # asyncio.run(post_request())
    # data = readAllAddressMaster()[0]
    # datafile = "datasource/POI_20240311_NON_DEL.xlsx"
    # df = pd.read_excel(datafile, engine="calamine")
    # df = pd.DataFrame(getDummyData()["data"])
    # insertExcelToPoiMaster(df)
    # ids = getDistinctAddressId()
    # getAllDataframe(5, ids)
    # df = getDataframeByAddressId("73b6439f85194a64a890bb5b53e95aff",5)
    # for column in df.columns:
    #     if column == 'addressId'\
    #         or column == 'isChineseAddress'\
    #             or column == "inputAddress"\
    #                 or column == "inputEasting"\
    #                     or column == "inputNorthing":
    #         df[column] = df[column].ffill()
    #     else:
    #         df[column] = df[column].fillna('Not applicable')
    
    # chinAddr = df[(df["isChineseAddress"] == True)]
    # engAddr = df[(df["isChineseAddress"] == False)]
    
    # # df["addressId"].fillna(method='ffill', inplace=True)
    # # df["isChineseAddress"].fillna(method='ffill', inplace=True)
    # # df["inputAddress"].fillna(method='ffill', inplace=True)
    # # df["inputAddress"].fillna(method='ffill', inplace=True)
    # print(df["geoDataChinAddress"], df["addressSearchChinAddress"])
    # with pd.ExcelWriter("output.xlsx") as writer:
    #     chinAddr.to_excel(writer, sheet_name = "chinAddress", freeze_panes=(1, 1))
    #     engAddr.to_excel(writer, sheet_name = "engAddress", freeze_panes=(1, 1))
    #     df.to_excel(writer, sheet_name = "all", freeze_panes=(1, 1))
    # rows = getCaseWithoutResult()
    # updateAddressResultByEndpoints("https://geodata.gov.hk/gs/api/v1.0.0/locationSearc","https://geodata.gov.hk/gs/api/v1.0.0/locationSearch")
    # rows = getRemainingMaster()
    ids = caseDetailWithCountLessThan(4)
    rows = getPoiMasterByIds(ids)
    endpoints = [Als(),GeoData()]
    # endpoints = [AddressSearch()]
    asyncio.run(main(rows, endpoints))