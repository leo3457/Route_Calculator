"""
Contains the CREATE TABLE schema, the UPSERT logic for database integrity, and the fleet-wide SELECT queries 
to identify active trucks and fetch their telemetry data. This module serves as the SQL query repository for 
the entire route calculation process, ensuring a single source of truth for all database interactions 
related to route extraction and storage.
"""

from sqlalchemy import text

# --- SCHEMA / TABLE CREATION ---
CREATE_TABLE_DRIVINGROUTE = text("""
CREATE TABLE IF NOT EXISTS driving_route (
    id UUID PRIMARY KEY,
    deviceid UUID NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    start_odo NUMERIC(10, 2),
    end_odo NUMERIC(10, 2),
    miles_driven NUMERIC(10, 2),
    soc_start NUMERIC(5, 2),
    soc_end NUMERIC(5, 2),
    split_reason TEXT,
    recordupdatedat TIMESTAMP DEFAULT NOW(),
    recordcreatedat TIMESTAMP DEFAULT NOW(),
    UNIQUE (deviceid, start_time)
);
""")

# --- UPSERT LOGIC ---
UPSERT_SQL_DRIVINGROUTE = text("""
    INSERT INTO driving_route (
        id, deviceid, start_time, end_time, 
        start_odo, end_odo, miles_driven, 
        soc_start, soc_end, split_reason
    )
    VALUES (
        :id, :deviceid, :start_time, :end_time, 
        :start_odo, :end_odo, :miles_driven, 
        :soc_start, :soc_end, :split_reason
    )
    ON CONFLICT (id)
    DO UPDATE SET
        end_time = EXCLUDED.end_time,
        start_odo = EXCLUDED.start_odo,
        end_odo = EXCLUDED.end_odo,
        miles_driven = EXCLUDED.miles_driven,
        soc_end = EXCLUDED.soc_end,
        split_reason = EXCLUDED.split_reason,
        recordupdatedat = NOW();
""")

SELECT_SQL_Activatedevices = text("""
    SELECT DISTINCT deviceid
    FROM golden_signal
    WHERE time::timestamp > NOW() - INTERVAL '48 hours'
""")

SELECT_SQL_GoldenSignals = text("""
    SELECT * FROM golden_signal 
    WHERE deviceid = :deviceid 
      AND time::timestamp >= NOW() - INTERVAL ':last hours' 
      AND time::timestamp < NOW()
    ORDER BY time ASC;
""")

SELECT_SQL_OverlappingRoutes = text("""
    SELECT id, deviceid, start_time, end_time, start_odo, end_odo, split_reason
    FROM driving_route
    WHERE deviceid = :deviceid
      AND (end_time >= :window_start OR split_reason = 'Open')
    ORDER BY start_time ASC;
""")

SELECT_SQL_ALLGoldenSignals = text("""
    SELECT * FROM golden_signal 
    WHERE time::timestamp >= :cutoff_time 
      AND time::timestamp < NOW()
    ORDER BY time ASC;
""")

SELECT_SQL_ALLOverlappingRoutes = text("""
    SELECT id, deviceid, start_time, end_time, start_odo, end_odo, split_reason
    FROM driving_route
    WHERE end_time >= :cutoff_time OR split_reason = 'Open'
""")