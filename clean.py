import duckdb
import os
import logging
import pandas as pd

os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename=os.path.join("logs", "clean.log"),
    filemode="a"
)
logger = logging.getLogger(__name__)

def get_duckdb_connection(db_path: str = "emissions.duckdb"):
    """
    Establish and return a DuckDB connection.
    """
    try:
        con = duckdb.connect(db_path)
        logging.info(f"Connected to DuckDB at {db_path}")
        return con
    except Exception as e:
        logging.error(f"Failed to connect to DuckDB: {e}")
        raise

def remove_duplicate_trips(con, table: str):
    """
    Remove duplicate trips based on key identifying fields.
    """
    try:
        # Get count before cleaning
        before_count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        
        # Remove duplicates, keeping the first occurrence
        # Using pickup/dropoff datetime, locations, distance, and passenger count as key fields
        pickup_col = "tpep_pickup_datetime" if table == "yellow" else "lpep_pickup_datetime"
        dropoff_col = "tpep_dropoff_datetime" if table == "yellow" else "lpep_dropoff_datetime"
        
        con.execute(f"""
            DELETE FROM {table}
            WHERE rowid NOT IN (
                SELECT MIN(rowid)
                FROM {table}
                GROUP BY {pickup_col}, {dropoff_col}, PULocationID, DOLocationID, 
                         trip_distance, passenger_count
            )
        """)
        
        after_count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        removed = before_count - after_count
        
        message = f"TABLES UPDATED TO REMOVE DUPLICATE TRIPS: {removed:,} duplicates removed from {table}"
        print(message)
        logging.info(message)
        
        return removed
        
    except Exception as e:
        logging.error(f"Error removing duplicates from {table}: {e}")
        raise

def remove_zero_passenger_trips(con, table: str):
    """
    Remove trips with 0 passengers.
    """
    try:
        before_count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        
        con.execute(f"""
            DELETE FROM {table}
            WHERE passenger_count = 0
        """)
        
        after_count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        removed = before_count - after_count
        
        message = f"TABLES UPDATED TO REMOVE TRIPS WITH 0 PASSENGERS: {removed:,} trips removed from {table}"
        print(message)
        logging.info(message)
        
        return removed
        
    except Exception as e:
        logging.error(f"Error removing zero passenger trips from {table}: {e}")
        raise

def remove_zero_mile_trips(con, table: str):
    """
    Remove trips with 0 miles distance.
    """
    try:
        before_count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        
        con.execute(f"""
            DELETE FROM {table}
            WHERE trip_distance = 0
        """)
        
        after_count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        removed = before_count - after_count
        
        message = f"TABLES UPDATED TO REMOVE TRIPS 0 MILES IN LENGTH: {removed:,} trips removed from {table}"
        print(message)
        logging.info(message)
        
        return removed
        
    except Exception as e:
        logging.error(f"Error removing zero mile trips from {table}: {e}")
        raise

def remove_long_distance_trips(con, table: str):
    """
    Remove trips greater than 100 miles in length.
    """
    try:
        before_count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        
        con.execute(f"""
            DELETE FROM {table}
            WHERE trip_distance > 100
        """)
        
        after_count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        removed = before_count - after_count
        
        message = f"TABLES UPDATED TO REMOVE TRIPS GREATER THAN 100 MILES IN LENGTH: {removed:,} trips removed from {table}"
        print(message)
        logging.info(message)
        
        return removed
        
    except Exception as e:
        logging.error(f"Error removing long distance trips from {table}: {e}")
        raise

def remove_long_duration_trips(con, table: str):
    """
    Remove trips greater than 24 hours in duration.
    """
    try:
        before_count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        
        pickup_col = "tpep_pickup_datetime" if table == "yellow" else "lpep_pickup_datetime"
        dropoff_col = "tpep_dropoff_datetime" if table == "yellow" else "lpep_dropoff_datetime"
        
        con.execute(f"""
            DELETE FROM {table}
            WHERE EXTRACT(EPOCH FROM ({dropoff_col} - {pickup_col})) > 86400
        """)
        
        after_count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        removed = before_count - after_count
        
        message = f"TABLES UPDATED TO REMOVE TRIPS GREATER THAN 24 HOURS IN LENGTH: {removed:,} trips removed from {table}"
        print(message)
        logging.info(message)
        
        return removed
        
    except Exception as e:
        logging.error(f"Error removing long duration trips from {table}: {e}")
        raise

def clean_table(con, table: str):
    """
    Run comprehensive cleaning SQL statements on the specified table.
    """
    try:
        print(f"\n=== CLEANING TABLE: {table.upper()} ===")
        logging.info(f"Starting comprehensive cleaning for table: {table}")
        
        # Track total removed across all operations
        total_removed = 0
        
        # 1. Remove duplicate trips
        removed = remove_duplicate_trips(con, table)
        total_removed += removed
        
        # 2. Remove trips with 0 passengers
        removed = remove_zero_passenger_trips(con, table)
        total_removed += removed
        
        # 3. Remove trips with 0 miles
        removed = remove_zero_mile_trips(con, table)
        total_removed += removed
        
        # 4. Remove trips greater than 100 miles
        removed = remove_long_distance_trips(con, table)
        total_removed += removed
        
        # 5. Remove trips greater than 24 hours
        removed = remove_long_duration_trips(con, table)
        total_removed += removed
        
        final_count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        
        print(f"\n=== CLEANING COMPLETE FOR {table.upper()} ===")
        print(f"Total trips removed: {total_removed:,}")
        print(f"Final trip count: {final_count:,}")
        
        logging.info(f"Cleaning complete for {table}: {total_removed:,} trips removed, {final_count:,} trips remaining")

    except Exception as e:
        logging.error(f"Error cleaning table {table}: {e}")
        raise

def verify_cleaning_conditions(con, table: str):
    """
    Verify that all cleaning conditions have been met.
    """
    try:
        print(f"\n=== VERIFICATION TESTS FOR {table.upper()} ===")
        logging.info(f"Running verification tests for table: {table}")
        
        pickup_col = "tpep_pickup_datetime" if table == "yellow" else "lpep_pickup_datetime"
        dropoff_col = "tpep_dropoff_datetime" if table == "yellow" else "lpep_dropoff_datetime"
        
        # Test 1: Check for duplicate trips
        duplicate_count = con.execute(f"""
            SELECT COUNT(*) - COUNT(DISTINCT {pickup_col}, {dropoff_col}, PULocationID, DOLocationID, trip_distance, passenger_count)
            FROM {table}
        """).fetchone()[0]
        
        test1_result = "PASS" if duplicate_count == 0 else "FAIL"
        message1 = f"Test 1 - Duplicate trips: {test1_result} ({duplicate_count} duplicates found)"
        print(message1)
        logging.info(message1)
        
        # Test 2: Check for 0 passenger trips
        zero_passenger_count = con.execute(f"""
            SELECT COUNT(*) FROM {table} WHERE passenger_count = 0
        """).fetchone()[0]
        
        test2_result = "PASS" if zero_passenger_count == 0 else "FAIL"
        message2 = f"Test 2 - Zero passenger trips: {test2_result} ({zero_passenger_count} found)"
        print(message2)
        logging.info(message2)
        
        # Test 3: Check for 0 mile trips
        zero_mile_count = con.execute(f"""
            SELECT COUNT(*) FROM {table} WHERE trip_distance = 0
        """).fetchone()[0]
        
        test3_result = "PASS" if zero_mile_count == 0 else "FAIL"
        message3 = f"Test 3 - Zero mile trips: {test3_result} ({zero_mile_count} found)"
        print(message3)
        logging.info(message3)
        
        # Test 4: Check for trips > 100 miles
        long_distance_count = con.execute(f"""
            SELECT COUNT(*) FROM {table} WHERE trip_distance > 100
        """).fetchone()[0]
        
        test4_result = "PASS" if long_distance_count == 0 else "FAIL"
        message4 = f"Test 4 - Trips > 100 miles: {test4_result} ({long_distance_count} found)"
        print(message4)
        logging.info(message4)
        
        # Test 5: Check for trips > 24 hours
        long_duration_count = con.execute(f"""
            SELECT COUNT(*) FROM {table} 
            WHERE EXTRACT(EPOCH FROM ({dropoff_col} - {pickup_col})) > 86400
        """).fetchone()[0]
        
        test5_result = "PASS" if long_duration_count == 0 else "FAIL"
        message5 = f"Test 5 - Trips > 24 hours: {test5_result} ({long_duration_count} found)"
        print(message5)
        logging.info(message5)
        
        # Overall result
        all_tests_pass = all([test1_result == "PASS", test2_result == "PASS", test3_result == "PASS", 
                             test4_result == "PASS", test5_result == "PASS"])
        overall_result = "ALL TESTS PASSED" if all_tests_pass else "SOME TESTS FAILED"
        
        print(f"\n=== OVERALL RESULT FOR {table.upper()}: {overall_result} ===")
        logging.info(f"Overall verification result for {table}: {overall_result}")
        
        return all_tests_pass
        
    except Exception as e:
        logging.error(f"Error running verification tests for {table}: {e}")
        raise

def main():
    """
    Main function to run comprehensive cleaning on all taxi tables.
    """
    try:
        print("=" * 60)
        print("STARTING COMPREHENSIVE DATA CLEANING PROCESS")
        print("=" * 60)
        logging.info("Starting comprehensive data cleaning process")
        
        # Connect to database
        con = get_duckdb_connection()
        
        # Tables to clean
        tables_to_clean = ["yellow", "green"]
        
        # Clean each table
        for table in tables_to_clean:
            try:
                # Check if table exists
                table_exists = con.execute(f"""
                    SELECT COUNT(*) FROM information_schema.tables 
                    WHERE table_name = '{table}'
                """).fetchone()[0]
                
                if table_exists == 0:
                    print(f"Table {table} does not exist, skipping...")
                    continue
                
                # Get initial count
                initial_count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                print(f"\nInitial count for {table}: {initial_count:,} trips")
                
                # Clean the table
                clean_table(con, table)
                
                # Verify cleaning conditions
                verify_cleaning_conditions(con, table)
                
            except Exception as e:
                print(f"Error processing table {table}: {e}")
                logging.error(f"Error processing table {table}: {e}")
                continue
        
        print("\n" + "=" * 60)
        print("DATA CLEANING PROCESS COMPLETED")
        print("=" * 60)
        logging.info("Data cleaning process completed")
        
    except Exception as e:
        print(f"Fatal error in main cleaning process: {e}")
        logging.error(f"Fatal error in main cleaning process: {e}")
        raise
    finally:
        if 'con' in locals():
            con.close()
            print("Database connection closed")

if __name__ == "__main__":
    main()
