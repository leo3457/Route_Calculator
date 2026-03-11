import unittest
import pandas as pd
from datetime import timedelta
from sqlalchemy import text
from unittest.mock import patch
import logging
import time

from dbhandler.dbhandler import get_dataframe_from_postgresql, save_csv_to_db, save_to_db, sqlStmt_to_db
from app import app

from api_resource.route_calculator.route_calculator2 import process_golden_df, route_calculation
from sql_query.route_queries import (
    UPSERT_SQL_DRIVINGROUTE,
    CREATE_TABLE_DRIVINGROUTE,
    SELECT_SQL_ALLOverlappingRoutes # Added this so we can patch it
)

# --- DYNAMICALLY POINT TO DRIVING_ROUTE2 ---
# This safely modifies the queries just for the test without changing your actual route_queries.py file
UPSERT_SQL_DRIVINGROUTE2 = text(str(UPSERT_SQL_DRIVINGROUTE).replace("driving_route", "driving_route2"))
CREATE_TABLE_DRIVINGROUTE2 = text(str(CREATE_TABLE_DRIVINGROUTE).replace("driving_route", "driving_route2"))
SELECT_SQL_ALLOverlappingRoutes2 = text(str(SELECT_SQL_ALLOverlappingRoutes).replace("driving_route", "driving_route2"))


class TestQueryLogic(unittest.TestCase):
    def setUp(self):
        self.client = app.test_client()
        self.client.testing = True

        # Silence the dbhandler INFO spam during tests
        logging.getLogger().setLevel(logging.WARNING)

        csv_path = "tests/sample_golden_signal_opent1.csv"
        save_csv_to_db(
            csv_path=csv_path,
            table_name="golden_signal",
            if_exists="replace",
            chunksize=5000,
        )

        # Ensure we drop/create the correct table
        sqlStmt_to_db(text("DROP TABLE IF EXISTS driving_route2;"))
        sqlStmt_to_db(CREATE_TABLE_DRIVINGROUTE2)

    # def test_engine_overlap_logic(self):
    #     print("\n========== TESTING THE MATH ENGINE (process_golden_df) ==========")
    #     test_start = time.perf_counter() # Start test stopwatch
        
    #     trucks_df = get_dataframe_from_postgresql(text("SELECT DISTINCT deviceid FROM golden_signal"))
    #     device_ids = trucks_df['deviceid'].tolist()
        
    #     trucks_with_routes = 0
    #     total_routes_across_fleet = 0

    #     for dev_id in device_ids:
    #         query = text(f"SELECT * FROM golden_signal WHERE deviceid = '{dev_id}' ORDER BY time ASC")
    #         df_full = get_dataframe_from_postgresql(query)
    #         df_full['time'] = pd.to_datetime(df_full['time'], format='mixed')
            
    #         if df_full.empty: continue

    #         min_time = df_full['time'].min()
    #         max_time = df_full['time'].max()
    #         midpoint = min_time + (max_time - min_time) / 2
            
    #         # --- RUN 1 ---
    #         mask1 = df_full['time'] <= (midpoint + timedelta(hours=1))
    #         df_run1 = df_full[mask1].copy()
    #         routes_run1 = process_golden_df(df_run1, existing_routes=[]) 
    #         if routes_run1: save_to_db(UPSERT_SQL_DRIVINGROUTE2, routes_run1) # Write to DB 2

    #         # --- RUN 2 ---
    #         mask2 = df_full['time'] >= (midpoint - timedelta(hours=1))
    #         df_run2 = df_full[mask2].copy()

    #         overlap_query = text(f"SELECT * FROM driving_route2 WHERE deviceid = '{dev_id}'")
    #         existing_routes_df = get_dataframe_from_postgresql(overlap_query)
    #         existing_routes = existing_routes_df.to_dict(orient='records') if not existing_routes_df.empty else []
            
    #         routes_run2 = process_golden_df(df_run2, existing_routes=existing_routes)
    #         if routes_run2: save_to_db(UPSERT_SQL_DRIVINGROUTE2, routes_run2) # Write to DB 2

    #         total = get_dataframe_from_postgresql(text(f"SELECT COUNT(*) as c FROM driving_route2 WHERE deviceid='{dev_id}'"))['c'].iloc[0]
    #         total_routes_across_fleet += total
            
    #         # ONLY print if this specific truck actually drove
    #         if total > 0:
    #             trucks_with_routes += 1
    #             print(f"  -> Truck {dev_id[:8]}... Saved {len(routes_run1)} (Run 1) | Saved {len(routes_run2)} (Run 2) | Total: {total}")

    #     test_end = time.perf_counter() # Stop test stopwatch
    #     print(f"\n  >>> VERIFICATION: Engine found routes for {trucks_with_routes} trucks.")
    #     print(f"  >>> VERIFICATION: Engine finalized {total_routes_across_fleet} TOTAL unique routes across fleet.")
    #     print(f"  >>> ENGINE TEST TOTAL TIME: {test_end - test_start:.3f} seconds <<<\n")


    # Patch the calculator's internal SQL variables with our modified 'driving_route2' versions
    @patch('api_resource.route_calculator.route_calculator2.SELECT_SQL_ALLOverlappingRoutes', SELECT_SQL_ALLOverlappingRoutes2)
    @patch('api_resource.route_calculator.route_calculator2.UPSERT_SQL_DRIVINGROUTE', UPSERT_SQL_DRIVINGROUTE2)
    @patch('api_resource.route_calculator.route_calculator2.get_dataframe_from_postgresql')
    def test_production_batch_wrapper(self, mock_get_df):
        print("========== TESTING THE BATCH WRAPPER (route_calculation) ==========")
        test_start = time.perf_counter() # Start test stopwatch
        
        full_data_query = text("SELECT * FROM golden_signal ORDER BY time ASC")
        df_full = get_dataframe_from_postgresql(full_data_query)
        df_full['time'] = pd.to_datetime(df_full['time'], format='mixed')
        
        min_time = df_full['time'].min()
        max_time = df_full['time'].max()
        midpoint = min_time + (max_time - min_time) / 2

        print(f"  [Run 1] Simulating Wrapper at {midpoint + timedelta(hours=1)}")
        mask1 = df_full['time'] <= (midpoint + timedelta(hours=1))
        df_run1 = df_full[mask1].copy()

        def mock_db_call_run1(query, params=None):
            if "SELECT * FROM golden_signal" in str(query): return df_run1
            elif "FROM driving_route2" in str(query): return pd.DataFrame() 
            return pd.DataFrame()

        mock_get_df.side_effect = mock_db_call_run1
        
        # Temporarily unmute logger so we can see the batch profiler metrics
        logging.getLogger().setLevel(logging.INFO)
        route_calculation(hours_back=100000)
        logging.getLogger().setLevel(logging.WARNING)

        verification_query = text("SELECT COUNT(*) as route_count FROM driving_route2")
        run1_total = get_dataframe_from_postgresql(verification_query)['route_count'].iloc[0]
        print(f"  -> Run 1 completed. Unique routes in DB: {run1_total}")

        print(f"\n  [Run 2] Simulating Wrapper at {max_time}")
        mask2 = df_full['time'] >= (midpoint - timedelta(hours=1))
        df_run2 = df_full[mask2].copy()

        def mock_db_call_run2(query, params=None):
            if "SELECT * FROM golden_signal" in str(query): return df_run2
            elif "FROM driving_route2" in str(query):
                from dbhandler.dbhandler import get_dataframe_from_postgresql as real_get_df
                return real_get_df(query, params) 
            return pd.DataFrame()

        mock_get_df.side_effect = mock_db_call_run2
        
        # Temporarily unmute logger again
        logging.getLogger().setLevel(logging.INFO)
        route_calculation(hours_back=100000)
        logging.getLogger().setLevel(logging.WARNING)

        final_total = get_dataframe_from_postgresql(verification_query)['route_count'].iloc[0]
        test_end = time.perf_counter() # Stop test stopwatch
        print(f"  -> Run 2 completed. Final unique routes in DB: {final_total}")
        print(f"  >>> BATCH WRAPPER TEST TOTAL TIME: {test_end - test_start:.3f} seconds <<<")
        print("===================================================================\n")