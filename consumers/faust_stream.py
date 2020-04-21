"""Defines trends calculations for stations"""
import logging

import faust


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


app = faust.App("stations-stream-1", broker="kafka://localhost:9092", store="memory://")
topic = app.topic("org.chicago.cta.stations", value_type=Station)
out_topic = app.topic("org.chicago.cta.stations.table.v2",value_type=TransformedStation ,partitions=1)

@app.agent(topic)
async def process(stream):
    async for station in stream:
        line = "red" if station.red else "blue" if station.blue else "green"
        transformed_station = TransformedStation(station.station_id, station.station_name, station.order, line)
        await out_topic.send(value = transformed_station)

if __name__ == "__main__":
    app.main()
