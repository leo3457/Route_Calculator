import pandas as pd
import uuid
from datetime import datetime, timezone
from utilities.util import generate_uuid
import logging
from dbhandler.dbhandler import get_dataframe_from_postgresql, save_to_db
from sql_query.route_queries import (
    SELECT_SQL_Activatedevices,
    SELECT_SQL_GoldenSignals,
    UPSERT_SQL_DRIVINGROUTE,
    SELECT_SQL_OverlappingRoutes
)
from flask_restful import Resource, Api

METERS_TO_MILES = 0.000621371

class RouteCalculation(Resource):
    ''''
    Harbinger Route Calculator

    Processes synchronized CAN telemetry from the golden_signal database table 
    into consolidated driving route reports, safely handling overlapping time windows.

    Input Format: 
    A Pandas DataFrame of synchronized telemetry with the following key columns:
    - time          -> Timestamp (UTC)
    - deviceid      -> Vehicle UUID
    - soc           -> Battery State of Charge (0-100)
    - odometer      -> Cumulative distance (meters)
    - charge_state  -> VCU Charging State (8=Active/ChargeStart)

    Core Route Logic & Split Reasons:
    - Route Starts: First odometer increase (> 0.01 miles) detected.
    - split_reason = 'Charge': The route naturally ends because the truck plugged in. 
      Triggered when charge_state == 8 for >= 15 consecutive records (noise filter).
    - split_reason = 'Idle': The route naturally ends because the truck parked. 
      Triggered when a time gap of >= 2.5 hours occurs between telemetry rows.
    - split_reason = 'Open': The truck was still actively driving when the data 
      window cut off. This route is saved to the database but flagged as unfinished.

    Architecture & Fault Tolerance:
    - Validation: Routes with a distance < 0.1 miles or > 150.0 miles are 
      filtered and discarded as signal noise/glitches.
    - Deterministic UUIDs: Primary keys are generated dynamically using a hash 
      of the deviceid and the route's exact start_time.
    - Window Reconciliation: Before finalizing calculations, the engine queries 
      the database for any historical overlapping routes. If a calculated route 
      overlaps with a database route, it inherits the database's original 
      start_time and start_odo. This guarantees that sliding window overlap 
      results in a clean PostgreSQL UPSERT, rather than duplicating the trip.
    '''
    def get(self):
        msg="This API call does not have a GET method"
        return {'error': msg}
    def put(self):
        msg="This API call does not have a PUT method"
        return {'error': msg}
    def post(self):
        logging.info("post route_calculation start")
        try:
            route_calculation()
        except:
            return {'message': 'error occured'}, 400

        return { "status": 'success' }, 200


def generate_uuid(deviceid, start_time):
    """Generates a deterministic UUID based on device ID and route start time."""
    unique_string = f"{deviceid}_{start_time.isoformat()}"
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, unique_string))



def route_calculation():
    logging.info("Starting Route Calculation Process...")
    
    # 2. Identify active trucks in in the 48-hour window
    active_trucks_df = get_dataframe_from_postgresql(SELECT_SQL_Activatedevices)

    if active_trucks_df.empty:
        logging.info("No active trucks found in the last 48 hours. Exiting.")
        return

    trucks_list = active_trucks_df['deviceid'].tolist()
    logging.info(f"Found {len(trucks_list)} trucks to process.")

    # 3. Process each truck's data sequentially (The Wrapper Loop)
    for device_id in trucks_list:
        logging.info(f"--- Processing truck {device_id} ---")

        try:
            # a. Define the 48-hour window for this truck
            params = {
                "deviceid": device_id,
                "last": 48
            }

            # b. Fetch the truck's telemetry data (wide-format golden_signal)
            df_raw = get_dataframe_from_postgresql(SELECT_SQL_GoldenSignals, params)

            if df_raw.empty:
                logging.info(f"No telemetry data found for truck {device_id} in the window. Skipping.")
                continue

            # Find the exact start time of the telemetry data we just pulled
            window_start = df_raw['time'].min()
            overlap_params = {
                "deviceid": device_id,
                "window_start": window_start
            }

            # Ask the DB for any routes that might overlap with our current window (including "Open" routes)
            existing_routes_df = get_dataframe_from_postgresql(SELECT_SQL_OverlappingRoutes, overlap_params)

            # Convert the df to a list of dictionaries for the calculator
            existing_routes = existing_routes_df.to_dict(orient='records') if not existing_routes_df.empty else []
            if existing_routes:
                logging.info(f"Found {len(existing_routes)} overlapping routes in the DB for truck {device_id}.")
        
            # 4. Run the Calculator Logic (k=15 noise filter)
            # This returns a list of dicts ready for upsert
            routes = process_golden_df(df_raw, existing_routes=existing_routes)

            if not routes:
                logging.info(f"No valid driving route identified for ({device_id}).")
                continue

            # 5. Batch Upsert to DB
            # This part handles both inserting new routes and updating "Open" routes
            save_to_db(UPSERT_SQL_DRIVINGROUTE, routes)
            logging.info(f"Successfully synced {len(routes)} routes for {device_id}.")

        except Exception as e:
            # Isolate the failure: log and move onto next truck
            logging.error(f"Error processing truck {device_id}: {e}")
            continue
        
    logging.info("Fleet Route Sync Process Completed.")

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
    
    # --- Strict Deduplication & Reset Index ---
    df = df.drop_duplicates(subset=['time']).sort_values('time').reset_index(drop=True)
    dev_id = str(df['deviceid'].iloc[0])    
    
    raw_routes = []
    
    current_route_start_time = None
    current_route_start_odo = None
    soc_start = None
    charge_steady_count = 0
    prev_row = None
    last_movement_time = None

    # --- 1. THE CALCULATION LOOP ---
    for _, row in df.iterrows():
        t = pd.to_datetime(row['time']) 
        odo_mi = row['odometer'] * METERS_TO_MILES
        soc = row['soc']
        charge_st = row['charge_state']
        prnd_st = row.get('prnd_state', 0)

        if prev_row is None:
            row_copy = row.copy()
            row_copy['time'] = t
            row_copy['odo_mi'] = odo_mi
            prev_row = row_copy
            continue

        prev_time = prev_row['time']
        prev_odo = prev_row['odo_mi']

        time_gap = (t - prev_time).total_seconds()

        # --- Distance Sanity Check (Glitch Rejection) ---
        # If the truck appears to be driving > 120 mph, it's a hardware glitch.
        if time_gap > 0:
            speed_mph = (odo_mi - prev_odo) / (time_gap / 3600)
            if speed_mph > 120:
                # Ignore the glitch, pretend odometer didn't move and goes back to use previous row
                odo_mi = prev_odo

        if charge_st == 8:
            charge_steady_count += 1
        else:
            charge_steady_count = 0

        is_steady_charging = charge_steady_count >= k_threshold

        # --- Gear-Aware Routing ---
        # prnd_state == 1 means Park. It MUST NOT be in Park to trigger movement.
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
            # Minimum distance raised to 1.5 to elimnate "Yard/Parking Moves"
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
                    "soc_end": prev_row['soc'],
                    "split_reason": reason
                })
            current_route_start_time = None
            current_route_start_odo = None
            soc_start = None
            last_movement_time = None

        if moving:
            if current_route_start_odo is None:
                # --- Strict Offline Wake-ups ---
                # If the gap was massive (e.g. > 1 hour), start exactly NOW, not hours ago
                if time_gap > 3600:
                    current_route_start_time = t
                    current_route_start_odo = odo_mi
                    soc_start = soc
                else:
                    current_route_start_time = prev_time
                    current_route_start_odo = prev_odo
                    soc_start = prev_row['soc']

                last_movement_time = t

        row_copy = row.copy()
        row_copy['time'] = t
        row_copy['odo_mi'] = odo_mi
        prev_row = row_copy

    if current_route_start_odo is not None:
        dist = prev_row['odo_mi'] - current_route_start_odo
        if dist > min_miles:
            raw_routes.append({
                "deviceid": dev_id,
                "start_time": current_route_start_time,
                "end_time": prev_row['time'],
                "start_odo": current_route_start_odo,
                "end_odo": prev_row['odo_mi'],
                "soc_start": soc_start,
                "soc_end": prev_row['soc'],
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
                # If DB route is open, its end time is effectively infinite for overlap purposes
                db_end = pd.to_datetime(db_r['end_time']).tz_localize(None) if pd.notnull(db_r.get('end_time')) else pd.Timestamp.max
                
                # --- Strict Boundaries for Overlaps ---
                # OVERLAP LOGIC: If calculated start is before DB end AND calculated end is after DB start
                if calc_start < db_end and calc_end > db_start:
                    matched_db_route = db_r
                    break 
        
        if matched_db_route:
            # INHERIT FROM DATABASE
            r['start_time'] = pd.to_datetime(matched_db_route['start_time'])
            r['start_odo'] = matched_db_route['start_odo']
            r['soc_start'] = matched_db_route.get('soc_start', r['soc_start'])

        # Recalculate distance based on true start
        r['miles_driven'] = r['end_odo'] - r['start_odo']
        # Finally, generate the UUID based on the reconciled start_time
        r['id'] = generate_uuid(r['deviceid'], r['start_time'])

        # Final Safety net: ensure reconciled routes still meet the minimum distance
        # (Allow 'Open' routes to be short because they are still in progess)
        if r['split_reason'] == 'Open' or r['miles_driven'] >= min_miles:
            final_routes.append(r)

    return final_routes