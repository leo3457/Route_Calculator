import unittest
import pandas as pd
from sql_query.route_queries import (
    SELECT_SQL_Activatedevices,
    SELECT_SQL_GoldenSignals,
    UPSERT_SQL_DRIVINGROUTE,
    SELECT_SQL_OverlappingRoutes,
    CREATE_TABLE_DRIVINGROUTE  
)
from dbhandler.dbhandler import get_dataframe_from_postgresql, save_csv_to_db, save_to_db, sqlStmt_to_db
from app import app
import os
from api_resource.route_calculator.route_calculator import process_golden_df
from sqlalchemy import text
from unittest.mock import patch

@unittest.skipIf(os.environ.get('SKIP_TESTROUTE_CAL') == 'true', "Skipping old route calculation test")
class TestQueryLogic(unittest.TestCase):
    def setUp(self):
        self.client = app.test_client()
        self.client.testing = True

        csv_path = "tests/sample_golden_signal_route_cal.csv"
        save_csv_to_db(
            csv_path=csv_path,
            table_name="golden_signal",
            if_exists="replace",
            chunksize=5000,
        )

        # Load driving_table
        sqlStmt_to_db(CREATE_TABLE_DRIVINGROUTE)
    '''
    @patch('api_resource.route_calculator.route_calculator.get_dataframe_from_postgresql')
    def test_production_overlap_no_cheating(self, mock_get_df):
        from api_resource.route_calculator.route_calculator import route_calculation
        from sql_query.route_queries import (
            SELECT_SQL_Activatedevices,
            SELECT_SQL_GoldenSignals,
            UPSERT_SQL_DRIVINGROUTE,
            SELECT_SQL_OverlappingRoutes  
        )
        from sqlalchemy import text
        import pandas as pd

        dev_id = "b5f98353-c811-484b-ab7f-3b51af4a4713"
        
        # 1. Setup Mock for Active Devices
        mock_active_devices = pd.DataFrame({'deviceid': [dev_id]})
        
        # 2. Get the full dataset
        df_full = get_dataframe_from_postgresql(SELECT_SQL_GoldenSignals, {"deviceid": dev_id, "last": 5000})
        df_full['time'] = pd.to_datetime(df_full['time'])

        # --- SIMULATE RUN 1 ---
        print("\n--- DEVICE 1 T1 Running Production Wrapper: Run 1 ---")
        mask1 = (df_full['time'] >= '2026-02-17 00:00:00') & (df_full['time'] <= '2026-02-19 20:00:00')
        df_run1 = df_full[mask1].copy()

        # Mock get_dataframe_from_postgresql to return specific data depending on the query
        def mock_db_fetch_run1(query, params=None):
            if query == SELECT_SQL_Activatedevices:
                return mock_active_devices
            elif query == SELECT_SQL_GoldenSignals:
                return df_run1
            elif query == SELECT_SQL_OverlappingRoutes:
                return pd.DataFrame() # No overlapping routes in Run 1
            return pd.DataFrame()

        mock_get_df.side_effect = mock_db_fetch_run1
        
        # Call the production wrapper!
        route_calculation()

        # Verify Run 1 saved correctly 
        verification_query = text(f"SELECT COUNT(*) as route_count FROM driving_route WHERE deviceid = '{dev_id}'")
        verification_df = get_dataframe_from_postgresql(verification_query)
        run1_total = verification_df['route_count'].iloc[0]
        print(f"Run 1 completed. Unique routes in DB: {run1_total}")

        # --- SIMULATE RUN 2 (The Overlap) ---
        print("\n--- DEVICE 1 T1 Running Production Wrapper: Run 2 ---")
        mask2 = (df_full['time'] >= '2026-02-17 18:00:00') & (df_full['time'] <= '2026-02-19 23:59:59')
        df_run2 = df_full[mask2].copy()
        
        # Now mock to actually hit the DB for overlapping routes
        def mock_db_fetch_run2(query, params=None):
            if query == SELECT_SQL_Activatedevices:
                return mock_active_devices
            elif query == SELECT_SQL_GoldenSignals:
                return df_run2
            elif query == SELECT_SQL_OverlappingRoutes:
                # Actually fetch the overlapping routes from the DB
                return get_dataframe_from_postgresql(SELECT_SQL_OverlappingRoutes, params)
            return pd.DataFrame()

        mock_get_df.side_effect = mock_db_fetch_run2

        # Call the production wrapper again
        route_calculation()

        # --- VERIFICATION ---
        verification_df = get_dataframe_from_postgresql(verification_query)
        final_total = verification_df['route_count'].iloc[0]
        print(f"\nFinal Verification. Total unique routes in DB: {final_total}")
        
        # If the production logic works perfectly, the total should be 3, just like the unit test.
        self.assertEqual(final_total, 3, "Production wrapper created duplicate routes!")
    
    def test_overlap_strategy(self):
        from sql_query.route_queries import SELECT_SQL_OverlappingRoutes
        from sqlalchemy import text
        
        dev_id = "b5f98353-c811-484b-ab7f-3b51af4a4713"
        df_full = get_dataframe_from_postgresql(SELECT_SQL_GoldenSignals, {"deviceid": dev_id, "last": 5000})
        df_full['time'] = pd.to_datetime(df_full['time'])
        
        print("\n--- DEVICE 1 T2 Simulating Run 1 ---")
        mask1 = (df_full['time'] >= '2026-02-17 00:00:00') & (df_full['time'] <= '2026-02-19 20:00:00')
        df_run1 = df_full[mask1].copy()
        
        # Run 1 has no existing routes
        routes_run1 = process_golden_df(df_run1, existing_routes=[]) 
        if routes_run1:
            save_to_db(UPSERT_SQL_DRIVINGROUTE, routes_run1)
            print(f"Run 1 Saved {len(routes_run1)} routes.")

        print("\n--- DEVICE 1 T2 Simulating Run 2 (The Overlap) ---")
        mask2 = (df_full['time'] >= '2026-02-17 18:00:00') & (df_full['time'] <= '2026-02-19 23:59:59')
        df_run2 = df_full[mask2].copy()
        
        # 1. Fetch overlapping routes from the database
        window_start = df_run2['time'].min()
        params = {"deviceid": dev_id, "window_start": window_start}
        existing_routes_df = get_dataframe_from_postgresql(SELECT_SQL_OverlappingRoutes, params)
        
        existing_routes = existing_routes_df.to_dict(orient='records') if not existing_routes_df.empty else []
        print(f"Found {len(existing_routes)} overlapping routes in the DB.")

        # 2. Pass them to the calculator
        routes_run2 = process_golden_df(df_run2, existing_routes=existing_routes)
        if routes_run2:
            save_to_db(UPSERT_SQL_DRIVINGROUTE, routes_run2)
            print(f"Run 2 Saved {len(routes_run2)} routes. (Reconciled with DB!)")

        # 3. Verification using your existing dbhandler!
        verification_query = text(f"SELECT COUNT(*) as route_count FROM driving_route WHERE deviceid = '{dev_id}'")
        verification_df = get_dataframe_from_postgresql(verification_query)
        total = verification_df['route_count'].iloc[0]
        print(f"\nVerification: Total unique routes in DB: {total}")
        
        # Assert to make the test automatically pass/fail based on correct logic
        self.assertEqual(total, 3, "Duplicate routes were created during the overlap!")

    @patch('api_resource.route_calculator.route_calculator.get_dataframe_from_postgresql')
    def test_production_overlap_no_cheating2(self, mock_get_df):
        save_csv_to_db(
            csv_path="tests/sample_golden_signal_drive.csv",
            table_name="golden_signal",
            if_exists="replace",
            chunksize=5000,
        )
        from api_resource.route_calculator.route_calculator import route_calculation
        from sql_query.route_queries import (
            SELECT_SQL_Activatedevices,
            SELECT_SQL_GoldenSignals,
            UPSERT_SQL_DRIVINGROUTE,
            SELECT_SQL_OverlappingRoutes  
        )
        from sqlalchemy import text
        import pandas as pd

        dev_id = "0188675a-d55e-4f09-84d9-795cb9c24a13"
        
        # 1. Setup Mock for Active Devices
        mock_active_devices = pd.DataFrame({'deviceid': [dev_id]})
        
        # 2. Get the full dataset
        df_full = get_dataframe_from_postgresql(SELECT_SQL_GoldenSignals, {"deviceid": dev_id, "last": 5000})
        df_full['time'] = pd.to_datetime(df_full['time'])

        # --- TEST 2 SIMULATE RUN 1 ---
        print("\n--- DEVICE 2 T1 Running Production Wrapper: Run 1 ---")
        mask1 = (df_full['time'] >= '2026-02-19 00:00:00') & (df_full['time'] <= '2026-02-21 20:00:00')
        df_run1 = df_full[mask1].copy()

        # Mock get_dataframe_from_postgresql to return specific data depending on the query
        def mock_db_fetch_run1(query, params=None):
            if query == SELECT_SQL_Activatedevices:
                return mock_active_devices
            elif query == SELECT_SQL_GoldenSignals:
                return df_run1
            elif query == SELECT_SQL_OverlappingRoutes:
                return pd.DataFrame() # No overlapping routes in Run 1
            return pd.DataFrame()

        mock_get_df.side_effect = mock_db_fetch_run1
        
        # Call the production wrapper!
        route_calculation()

        # Verify Run 1 saved correctly 
        verification_query = text(f"SELECT COUNT(*) as route_count FROM driving_route WHERE deviceid = '{dev_id}'")
        verification_df = get_dataframe_from_postgresql(verification_query)
        run1_total = verification_df['route_count'].iloc[0]
        print(f"Run 1 completed. Unique routes in DB: {run1_total}")

        # --- DEVICE 2 SIMULATE RUN 2 (The Overlap) ---
        print("\n--- DEVICE 2 T1 Running Production Wrapper: Run 2 ---")
        mask2 = (df_full['time'] >= '2026-02-21 18:00:00') & (df_full['time'] <= '2026-02-25 23:59:59')
        df_run2 = df_full[mask2].copy()
        
        # Now mock to actually hit the DB for overlapping routes
        def mock_db_fetch_run2(query, params=None):
            if query == SELECT_SQL_Activatedevices:
                return mock_active_devices
            elif query == SELECT_SQL_GoldenSignals:
                return df_run2
            elif query == SELECT_SQL_OverlappingRoutes:
                # Actually fetch the overlapping routes from the DB
                return get_dataframe_from_postgresql(SELECT_SQL_OverlappingRoutes, params)
            return pd.DataFrame()

        mock_get_df.side_effect = mock_db_fetch_run2

        # Call the production wrapper again
        route_calculation()

        # --- VERIFICATION ---
        verification_df = get_dataframe_from_postgresql(verification_query)
        final_total = verification_df['route_count'].iloc[0]
        print(f"\nFinal Verification. Total unique routes in DB: {final_total}")
        
    
    def test_overlap_strategy2(self):
        save_csv_to_db(
            csv_path="tests/sample_golden_signal_drive.csv",
            table_name="golden_signal",
            if_exists="replace",
            chunksize=5000,
        )
        from sql_query.route_queries import SELECT_SQL_OverlappingRoutes
        from sqlalchemy import text
        
        dev_id = "0188675a-d55e-4f09-84d9-795cb9c24a13"
        df_full = get_dataframe_from_postgresql(SELECT_SQL_GoldenSignals, {"deviceid": dev_id, "last": 5000})
        df_full['time'] = pd.to_datetime(df_full['time'])
        
        print("\n--- DEVICE 2 T1 Simulating Run 1 ---")
        mask1 = (df_full['time'] >= '2026-02-1 00:00:00') & (df_full['time'] <= '2026-02-21 20:00:00')
        df_run1 = df_full[mask1].copy()
        
        # Run 1 has no existing routes
        routes_run1 = process_golden_df(df_run1, existing_routes=[]) 
        if routes_run1:
            save_to_db(UPSERT_SQL_DRIVINGROUTE, routes_run1)
            print(f"Run 1 Saved {len(routes_run1)} routes.")

        print("\n--- DEVICE 2 T1 Simulating Run 2 (The Overlap) ---")
        mask2 = (df_full['time'] >= '2026-02-21 18:00:00') & (df_full['time'] <= '2026-02-25 23:59:59')
        df_run2 = df_full[mask2].copy()

        # 1. Fetch overlapping routes from the database
        window_start = df_run2['time'].min()
        params = {"deviceid": dev_id, "window_start": window_start}
        existing_routes_df = get_dataframe_from_postgresql(SELECT_SQL_OverlappingRoutes, params)
        
        existing_routes = existing_routes_df.to_dict(orient='records') if not existing_routes_df.empty else []
        print(f"Found {len(existing_routes)} overlapping routes in the DB.")

        # 2. Pass them to the calculator
        routes_run2 = process_golden_df(df_run2, existing_routes=existing_routes)
        if routes_run2:
            save_to_db(UPSERT_SQL_DRIVINGROUTE, routes_run2)
            print(f"Run 2 Saved {len(routes_run2)} routes. (Reconciled with DB!)")

        # 3. Verification using your existing dbhandler!
        verification_query = text(f"SELECT COUNT(*) as route_count FROM driving_route WHERE deviceid = '{dev_id}'")
        verification_df = get_dataframe_from_postgresql(verification_query)
        total = verification_df['route_count'].iloc[0]
        print(f"\nVerification: Total unique routes in DB: {total}")
        
    '''
    
    def test_all_devices_overlap_strategy(self):
        # 1. Load your test CSV (Update path to whichever file you want to test)
        save_csv_to_db(
            csv_path="tests/sample_golden_signal_opent1.csv",
            table_name="golden_signal",
            if_exists="replace",
            chunksize=5000,
        )
        
        from sql_query.route_queries import SELECT_SQL_OverlappingRoutes
        from sqlalchemy import text
        import pandas as pd
        from datetime import timedelta
        
        # 2. Fetch every unique device ID that exists in this specific CSV
        distinct_query = text("SELECT DISTINCT deviceid FROM golden_signal")
        trucks_df = get_dataframe_from_postgresql(distinct_query, {})
        device_ids = trucks_df['deviceid'].tolist()
        
        print(f"\n========== TESTING OVERLAP STRATEGY FOR {len(device_ids)} DEVICES ==========")

        # 3. Iterate through all devices dynamically
        for dev_id in device_ids:
            print(f"\n--- Processing Device: {dev_id} ---")
            df_full = get_dataframe_from_postgresql(SELECT_SQL_GoldenSignals, {"deviceid": dev_id, "last": 5000})
            df_full['time'] = pd.to_datetime(df_full['time'])
            
            if df_full.empty:
                print(f"  -> No telemetry found, skipping.")
                continue
            
            # --- DYNAMIC TIMEFRAME CALCULATION ---
            min_time = df_full['time'].min()
            max_time = df_full['time'].max()
            
            # Find the exact middle of the truck's data timeline
            midpoint = min_time + (max_time - min_time) / 2
            
            # Create a 2-hour overlap across the midpoint
            overlap_start = midpoint - timedelta(hours=1)
            overlap_end = midpoint + timedelta(hours=1)

            print(f"  Timeline: {min_time}  -->  {max_time}")

            # --- RUN 1 (Start to Midpoint + 1hr) ---
            print(f"  [Run 1] {min_time} to {overlap_end}")
            mask1 = (df_full['time'] >= min_time) & (df_full['time'] <= overlap_end)
            df_run1 = df_full[mask1].copy()
            
            routes_run1 = process_golden_df(df_run1, existing_routes=[]) 
            if routes_run1:
                save_to_db(UPSERT_SQL_DRIVINGROUTE, routes_run1)
                print(f"  -> Run 1 Saved {len(routes_run1)} routes.")
            else:
                print("  -> Run 1 Saved 0 routes.")

            # --- RUN 2 (Midpoint - 1hr to End) ---
            print(f"  [Run 2] {overlap_start} to {max_time} (Overlap window active)")
            mask2 = (df_full['time'] >= overlap_start) & (df_full['time'] <= max_time)
            df_run2 = df_full[mask2].copy()

            if df_run2.empty:
                print("  -> Error: df_run2 is empty during overlap.")
                continue

            window_start = df_run2['time'].min()
            params = {"deviceid": dev_id, "window_start": window_start}
            existing_routes_df = get_dataframe_from_postgresql(SELECT_SQL_OverlappingRoutes, params)
            existing_routes = existing_routes_df.to_dict(orient='records') if not existing_routes_df.empty else []
            print(f"  -> Found {len(existing_routes)} overlapping routes in the DB to reconcile.")

            routes_run2 = process_golden_df(df_run2, existing_routes=existing_routes)
            if routes_run2:
                save_to_db(UPSERT_SQL_DRIVINGROUTE, routes_run2)
                print(f"  -> Run 2 Saved {len(routes_run2)} routes. (Reconciled with DB!)")
            else:
                print("  -> Run 2 Saved 0 routes.")

            # --- VERIFICATION ---
            verification_query = text(f"SELECT COUNT(*) as route_count FROM driving_route WHERE deviceid = '{dev_id}'")
            verification_df = get_dataframe_from_postgresql(verification_query)
            total = verification_df['route_count'].iloc[0]
            print(f"  >>> VERIFICATION: Total unique routes in DB for {dev_id}: {total} <<<\n")
            
        print("========== FLEET TEST COMPLETE ==========\n")
            

if __name__ == '__main__':
    unittest.main()