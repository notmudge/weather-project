import os, json
from datetime import date, timedelta
import pyodbc
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv

load_dotenv()

STORAGE_CONN_STR = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
SQL_CONN_STR     = os.environ["AZURE_SQL_CONNECTION_STRING"]

CREATE_STAGING = """
IF NOT EXISTS (
    SELECT * FROM sysobjects WHERE name='bronze_weather_raw' AND xtype='U'
)
CREATE TABLE bronze_weather_raw (
    id              BIGINT IDENTITY PRIMARY KEY,
    location_name   VARCHAR(50)   NOT NULL,
    observation_dt  DATE          NOT NULL,
    hour_utc        TINYINT       NOT NULL,
    temperature_2m  FLOAT,
    apparent_temp   FLOAT,
    precipitation   FLOAT,
    rain            FLOAT,
    snowfall        FLOAT,
    windspeed_10m   FLOAT,
    windgusts_10m   FLOAT,
    cloudcover      FLOAT,
    weathercode     INT,
    humidity_2m     FLOAT,
    ingested_at     DATE          NOT NULL,
    CONSTRAINT uq_obs UNIQUE (location_name, observation_dt, hour_utc)
);
"""

def parse_hourly_rows(payload):
    h    = payload["hourly"]
    meta = payload["_meta"]
    rows = []
    for i, ts in enumerate(h["time"]):
        obs_date, hour = ts.split("T")
        rows.append({
            "location_name": meta["location_name"],
            "observation_dt": obs_date,
            "hour_utc":       int(hour.split(":")[0]),
            "temperature_2m": h.get("temperature_2m",    [None]*24)[i],
            "apparent_temp":  h.get("apparent_temperature",[None]*24)[i],
            "precipitation":  h.get("precipitation",      [None]*24)[i],
            "rain":           h.get("rain",               [None]*24)[i],
            "snowfall":       h.get("snowfall",           [None]*24)[i],
            "windspeed_10m":  h.get("windspeed_10m",      [None]*24)[i],
            "windgusts_10m":  h.get("windgusts_10m",      [None]*24)[i],
            "cloudcover":     h.get("cloudcover",         [None]*24)[i],
            "weathercode":    h.get("weathercode",        [None]*24)[i],
            "humidity_2m":    h.get("relativehumidity_2m",[None]*24)[i],
            "ingested_at":    meta["ingested_at"],
        })
    return rows

def main():
    target_date = date.today() - timedelta(days=1)
    blob_svc    = BlobServiceClient.from_connection_string(STORAGE_CONN_STR)
    prefix      = (
        f"weather/year={target_date.year}"
        f"/month={target_date.month:02d}"
        f"/day={target_date.day:02d}/"
    )

    container = blob_svc.get_container_client("bronze")
    blobs     = list(container.list_blobs(name_starts_with=prefix))

    if not blobs:
        print(f"No blobs found for {target_date} — run ingest_weather.py first")
        return

    conn   = pyodbc.connect(SQL_CONN_STR)
    cursor = conn.cursor()
    cursor.execute(CREATE_STAGING)
    conn.commit()

    insert_sql = """
    MERGE bronze_weather_raw AS tgt
    USING (SELECT ?,?,?,?,?,?,?,?,?,?,?,?,?,?) AS src (
        location_name, observation_dt, hour_utc,
        temperature_2m, apparent_temp, precipitation,
        rain, snowfall, windspeed_10m, windgusts_10m,
        cloudcover, weathercode, humidity_2m, ingested_at
    )
    ON tgt.location_name = src.location_name
       AND tgt.observation_dt = src.observation_dt
       AND tgt.hour_utc = src.hour_utc
    WHEN NOT MATCHED THEN INSERT VALUES (
        src.location_name, src.observation_dt, src.hour_utc,
        src.temperature_2m, src.apparent_temp, src.precipitation,
        src.rain, src.snowfall, src.windspeed_10m, src.windgusts_10m,
        src.cloudcover, src.weathercode, src.humidity_2m, src.ingested_at
    );
    """

    for blob in blobs:
        data    = container.download_blob(blob.name).readall()
        payload = json.loads(data)
        rows    = parse_hourly_rows(payload)
        cursor.executemany(insert_sql, [list(r.values()) for r in rows])
        conn.commit()
        print(f"  ✓ loaded {len(rows)} rows from {blob.name}")

    cursor.close()
    conn.close()
    print("Done.")

if __name__ == "__main__":
    main()
