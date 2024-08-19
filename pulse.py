import tibber.const
import asyncio
import aiohttp
import tibber
import datetime
from influxdb_client import Point, InfluxDBClient, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS

def loadStringFromFile(filename):
    with open(filename, 'r') as file:
        return file.read().rstrip()


def _callback(pkg):
    data = pkg.get("data").get("liveMeasurement")
    if data is None:
        return
    
    point = Point("liveMeasurement").\
        tag("location", tibberAgent).\
        field("accumulatedConsumption", float(data.get("accumulatedConsumption"))).\
        field("accumulatedConsumptionLastHour", float(data.get("accumulatedConsumptionLastHour"))).\
        field("averagePower", float(data.get("averagePower"))).\
        field("lastMeterConsumption", float(data.get("lastMeterConsumption"))).\
        field("maxPower", float(data.get("maxPower"))).\
        field("minPower", float(data.get("minPower"))).\
        field("power", float(data.get("power"))).\
        field("estimatedHourConsumption", float(data.get("estimatedHourConsumption"))).\
        time(data.get("timestamp"))

    print(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), " - ", point.to_line_protocol())

    write_api.write(influxdbBucket, influxdbOrg, point)


async def run():
    async with aiohttp.ClientSession() as session:
        tibber_connection = tibber.Tibber(tibberToken, user_agent=tibberAgent, time_zone=tibberTimeZone)
        await tibber_connection.update_info()

    home = tibber_connection.get_homes(False)[0]
    await home.rt_subscribe(_callback)    

    while True:
      await asyncio.sleep(10)


tibberToken = loadStringFromFile(".env.tibber-token")
tibberAgent = loadStringFromFile(".env.tibber-agent")
tibberTimeZone = None

influxdbToken = loadStringFromFile(".env.influxdb-token")
influxdbOrg = loadStringFromFile(".env.influxdb-org")
influxdbBucket = loadStringFromFile(".env.influxdb-bucket")
influxdbUrl = loadStringFromFile(".env.influxdb-url")

client = InfluxDBClient(url=influxdbUrl, token=influxdbToken, org=influxdbOrg)
write_api = client.write_api(write_options=SYNCHRONOUS)


loop = asyncio.run(run())