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


app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")

stations_topic = app.topic("org.chicago.cta.stations", value_type=Station)

out_topic = app.topic("org.chicago.cta.stations.transformed", partitions=1)

stations_transformed_table = app.Table(
   "stations-transformed-table",
   default=list,
   partitions=1,
   changelog_topic=out_topic,
)

@app.agent(stations_topic)
async def stations(stations):
    async for st in stations:
        stations_transformed_table['station_id'] = {
            'station_id': st.station_id,
            'station_name': st.station_name,
            'order': st.order,
            'line': get_color(st)
        }

def get_color(station):
    if station.red:
        return 'red'
    if station.blue:
        return 'blue'
    if station.green:
        return 'green'

if __name__ == "__main__":
    app.main()
