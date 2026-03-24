import pandas as pd
import uuid
import time
from datetime import datetime, timezone, timedelta
from utilities.util import generate_uuid
import logging
from dbhandler.dbhandler import get_dataframe_from_postgresql, save_to_db
from sql_query.route_queries import (
    SELECT_SQL_Activatedevices,
    SELECT_SQL_ALLGoldenSignals,
    UPSERT_SQL_DRIVINGROUTE,
    SELECT_SQL_ALLOverlappingRoutes
)
from flask_restful import Resource, Api
from flask import request

METERS_TO_MILES = 0.000621371

class RouteCalculation(Resource):
    def get(self):
        msg="This API call does not have a GET method"
        return {'error': msg}
    def put(self):
        msg="This API call does not have a PUT method"
        return {'error': msg}
    def post(self):
        logging.info("post route_calculation start")
        try:
            hours_raw = request.args.get('hours', 48)

            # 2. Query params always come in as strings, so cast to int
            try:
                hours = int(hours_raw)
            except ValueError:
                return {'error': 'hours must be a number'}, 400
            
            route_calculation(hours_back=hours)
        except:
            return {'message': 'error occured'}, 400

        return { "status": 'success' }, 200


def generate_uuid(deviceid, start_time):
    """Generates a deterministic UUID based on device ID and route start time."""
    unique_string = f"{deviceid}_{start_time.isoformat()}"
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, unique_string))

def route_calculation(hours_back=48):
    print(hours_back)
    logging.info("Starting Route Calculation Process (Batch Mode)...")
    total_start_time = time.perf_counter() # Start the master stopwatch
    
    # 1. Define the cutoff time
    cutoff_time = datetime.now(timezone.utc) - timedelta(hours=hours_back)
    params = {"cutoff_time": cutoff_time}

    # --- PHASE 1: BULK DATABASE READS ---
    db_read_start = time.perf_counter()
    # Fetch ALL telemetry for ALL trucks in one query
    df_raw_all = get_dataframe_from_postgresql(SELECT_SQL_ALLGoldenSignals, params)
    
    if df_raw_all.empty:
        logging.info("No telemetry data found in the window. Exiting.")
        return

    # Fetch ALL overlapping routes for ALL trucks in one query
    existing_routes_df = get_dataframe_from_postgresql(SELECT_SQL_ALLOverlappingRoutes, params)
    db_read_end = time.perf_counter()
    logging.info(f"[Profiler] Bulk DB Reads completed in {db_read_end - db_read_start:.3f} seconds.")

    # --- PHASE 2: IN-MEMORY SECTIONING ---
    section_start = time.perf_counter()

    # Create a dictionary of overlapping routes keyed by deviceid for instant lookup
    # Format: {'truck_A_uuid': [route1, route2], 'truck_B_uuid': [route3]}
    routes_by_device = {}
    if not existing_routes_df.empty:
        for dev_id, group in existing_routes_df.groupby('deviceid'):
            routes_by_device[str(dev_id)] = group.to_dict(orient='records')

    # "Section" the massive telemetry dataframe into individual truck chunks
    grouped_telemetry = df_raw_all.groupby('deviceid')
    section_end = time.perf_counter()
    logging.info(f"[Profiler] Data sectioned into {len(grouped_telemetry)} trucks in {section_end - section_start:.3f} seconds.")
    
# --- PHASE 3: CALCULATION ENGINE ---
    calc_start = time.perf_counter()
    all_routes_to_save = []
    
    for device_id, df_truck in grouped_telemetry:
        try:
            device_id_str = str(device_id)
            
            # grab this truck's overlaps from our Python dictionary
            truck_existing_routes = routes_by_device.get(device_id_str, [])
            routes = process_golden_df(df_truck, existing_routes=truck_existing_routes)

            if routes:
                all_routes_to_save.extend(routes) # Add to the master list

        except Exception as e:
            logging.error(f"Error processing truck {device_id_str}: {e}")
            continue
            
    calc_end = time.perf_counter()
    logging.info(f"[Profiler] Calculated {len(all_routes_to_save)} total routes in {calc_end - calc_start:.3f} seconds.")

    # --- PHASE 4: BULK DATABASE WRITE ---
    write_start = time.perf_counter()
    
    if all_routes_to_save:
        save_to_db(UPSERT_SQL_DRIVINGROUTE, all_routes_to_save)
        
    write_end = time.perf_counter()
    logging.info(f"[Profiler] Bulk Database Write completed in {write_end - write_start:.3f} seconds.")

    # --- FINAL WRAP UP ---
    total_end_time = time.perf_counter()
    logging.info(f"Fleet Route Sync Process Completed in {total_end_time - total_start_time:.3f} seconds.")

    
def format_for_db(route_dict):
    now = datetime.now(timezone.utc)
    route_dict.pop('start_odo', None)
    route_dict.pop('end_odo', None)
    route_dict['recordcreatedat'] = now
    route_dict['recordupdatedat'] = now
    return route_dict

def process_golden_df(df, existing_routes=None, k_threshold=15, idle_hours=2.5, min_miles=1.5):
    if df.empty:
        return []

    # --- Format Time ---
    df['time'] = pd.to_datetime(df['time'], format='mixed')
    
    # --- Sort and Clean FIRST ---
    df = df.drop_duplicates(subset=['time']).sort_values(by='time').reset_index(drop=True)
    dev_id = str(df['deviceid'].iloc[0])

    # --- Vectorization (The Math) ---
    df['odo_mi'] = df['odometer'] * METERS_TO_MILES
    df['time_gap'] = df['time'].diff().dt.total_seconds().fillna(0.0)
    
    if 'prnd_state' not in df.columns:
        df['prnd_state'] = 0  # Default to 0 if column is missing

    raw_routes = []

    current_route_start_time = None
    current_route_start_odo = None
    soc_start = None
    charge_steady_count = 0
    last_movement_time = None
    
    # Primitive variables
    prev_time = None
    prev_odo = None
    prev_soc = None

    # --- THE CALCULATION LOOP ---
    for row in df.itertuples():
        t = row.time
        odo_mi = row.odo_mi
        soc = row.soc
        charge_st = row.charge_state
        prnd_st = row.prnd_state
        time_gap = row.time_gap

        if prev_time is None:
            prev_time = t
            prev_odo = odo_mi
            prev_soc = soc
            continue

        # --- Distance Sanity Check (Glitch Rejection) ---
        if time_gap > 0:
            speed_mph = (odo_mi - prev_odo) / (time_gap / 3600)
            if speed_mph > 120 or speed_mph < 0:
                odo_mi = prev_odo

        if charge_st == 8:
            charge_steady_count += 1
        else:
            charge_steady_count = 0

        is_steady_charging = charge_steady_count >= k_threshold

        # --- Gear-Aware Routing ---
        moving = (odo_mi - prev_odo) > 0.01 and prnd_st != 1
        if moving: 
            last_movement_time = t
        
        is_idling = False
        if time_gap >= idle_hours * 3600:
            is_idling = True
        elif last_movement_time is not None and (t - last_movement_time).total_seconds() >= idle_hours * 3600:
            is_idling = True

        if (is_steady_charging or is_idling) and current_route_start_odo is not None:
            dist = prev_odo - current_route_start_odo
            # Check minimum distance to eliminate "Yard Moves"
            if min_miles <= dist <= 300.0:
                reason = "Charge" if is_steady_charging else "Idle"
                route_end_time = last_movement_time if reason == "Idle" else prev_time
                
                raw_routes.append({
                    "deviceid": dev_id,
                    "start_time": current_route_start_time,
                    "end_time": route_end_time,
                    "start_odo": current_route_start_odo,
                    "end_odo": prev_odo,
                    "soc_start": soc_start,
                    "soc_end": prev_soc,
                    "split_reason": reason
                })
            current_route_start_time = None
            current_route_start_odo = None
            soc_start = None
            last_movement_time = None

        if moving:
            if current_route_start_odo is None:
                # --- Strict Offline Wake-ups ---
                if time_gap > 300:
                    current_route_start_time = t
                    current_route_start_odo = odo_mi
                    soc_start = soc
                else:
                    current_route_start_time = prev_time
                    current_route_start_odo = prev_odo
                    soc_start = prev_soc

                last_movement_time = t

        # Update primitive variables for the next iteration
        prev_time = t
        prev_odo = odo_mi
        prev_soc = soc

    # --- The Orphan Fix (Open Routes) ---
    if current_route_start_odo is not None:
        dist = prev_odo - current_route_start_odo
        if dist >= min_miles:
            raw_routes.append({
                "deviceid": dev_id,
                "start_time": current_route_start_time,
                "end_time": prev_time,
                "start_odo": current_route_start_odo,
                "end_odo": prev_odo,
                "soc_start": soc_start,
                "soc_end": prev_soc,
                "split_reason": "Open" 
            })

    # --- 2. WINDOW RECONCILIATION ---
    final_routes = []
    for r in raw_routes:
        calc_start = pd.to_datetime(r['start_time']).tz_localize(None)
        calc_end = pd.to_datetime(r['end_time']).tz_localize(None)
        
        matched_db_route = None
        
        if existing_routes:
            for db_r in existing_routes:
                db_start = pd.to_datetime(db_r['start_time']).tz_localize(None)
                db_end = pd.to_datetime(db_r['end_time']).tz_localize(None) if pd.notnull(db_r.get('end_time')) else pd.Timestamp.max
                
                if calc_start <= db_end and calc_end >= db_start:
                    matched_db_route = db_r
                    break 
        
        if matched_db_route:
            r['start_time'] = pd.to_datetime(matched_db_route['start_time'])
            r['start_odo'] = matched_db_route['start_odo']
            r['soc_start'] = matched_db_route.get('soc_start', r['soc_start'])

        r['miles_driven'] = r['end_odo'] - r['start_odo']
        r['id'] = generate_uuid(r['deviceid'], r['start_time'])

        if r['split_reason'] == 'Open' or r['miles_driven'] >= min_miles:
            final_routes.append(r)

    return final_routes