from furl import furl
from asynciolimiter import Limiter
from logger import*
import time
import aiohttp
import re

rate_limiter = Limiter(40/1)

class Endpoint:
    def __init__(self, base_url):
        self.base_url = base_url
        self.START = time.monotonic()

    async def fetchOne(self, client, row):
        await rate_limiter.wait()
        header = {"Accept" : "application/json",
                  "Content-Type" : "application/json"}
        address = row.address
        try:
            if hasattr(self, "is_post") and self.is_post:
                request_body = {"address": [address]}
                url = self.base_url
                async with client.post(url, json = request_body, ssl = False) as resp:
                    now = time.monotonic() - self.START
                    if resp.status == 200:
                        data = await resp.json()
                        logging.info(f"{now:.0f}s: {self.base_url} got {resp.status} querying {address}")
                    else:
                        data = None
                        logging.error(f"{now:.0f}s: {self.base_url} got {resp.status} querying {address}")
                    return data, resp.status
            else:
                url = furl(self.base_url).add({"q": address}).url
                async with client\
                    .get(url, ssl = False, headers = header) as resp:
                    now = time.monotonic() - self.START
                    if resp.status == 200:
                        data = await resp.json()
                        if self.base_url == "https://geodata.gov.hk/gs/api/v1.0.0/locationSearch":
                            for _ in data:
                                _["nameEN"] = re.sub(r"\n","",_["nameEN"])
                                _["addressEN"] = re.sub(r"\n","",_["addressEN"])
                                _["addressEN"] = re.sub("[^\x20-\x7E]", "", _['addressEN'])
                        logging.info(f"{now:.0f}s: {self.base_url} got {resp.status} querying {address}")
                    else:
                        data = None
                        logging.error(f"{now:.0f}s: {self.base_url} got {resp.status} querying {address}")
                    return data, resp.status
        except aiohttp.ClientConnectionError as e:
            logging.error(f"Request failed: {e}")
        return None, 808

class GeoData(Endpoint):
    def __init__(self):
        super().__init__("https://geodata.gov.hk/gs/api/v1.0.0/locationSearch")

class Als(Endpoint):
    def __init__(self):
        super().__init__("https://www.als.ogcio.gov.hk/lookup")
        
class AddressSearch(Endpoint):
    def __init__(self):
        super().__init__("http://10.77.242.157:8888/query_debug")
        self.is_post = True