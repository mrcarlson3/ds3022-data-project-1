import duckdb
import logging
import os
import matplotlib.pyplot as plt
import pandas as pd
from datetime import datetime

os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename=os.path.join("logs", "analysis.log"),
    filemode="a"
)
logger = logging.getLogger(__name__)

def get_connection(db_path: str = "emissions.duckdb"):
    """Opens a DuckDB connection to the emissions database and logs the result, 
    with error handling if the connection fails."""
    try:
        con = duckdb.connect(db_path)
        logger.info(f"Connected to DuckDB: {db_path}")
        return con
    except Exception as e:
        logger.error(f"Connection failed: {e}")
        raise

def get_largest_carbon_trips(con):
    """Finds the single trip with the highest CO2 emissions for each taxi type 
    and returns key trip details."""

    try:
        query = """
        WITH ranked_trips AS (
            SELECT 
                taxi_type,
                co2_per_trip_kg,
                trip_distance,
                trip_hour,
                trip_day_of_week,
                week_number,
                month,
                ROW_NUMBER() OVER (PARTITION BY taxi_type ORDER BY co2_per_trip_kg DESC) as rn
            FROM emissions.main.taxi
            WHERE co2_per_trip_kg IS NOT NULL
        )
        SELECT 
            taxi_type,
            co2_per_trip_kg,
            trip_distance,
            trip_hour,
            trip_day_of_week,
            week_number,
            month
        FROM ranked_trips
        WHERE rn = 1
        ORDER BY taxi_type
        """
        
        results = con.execute(query).fetchall()
        logger.info("Retrieved largest carbon trips")
        return results
    except Exception as e:
        logger.error(f"Error getting largest carbon trips: {e}")
        raise

def get_hourly_carbon_analysis(con):
    """Aggregates CO2 emissions by hour of day for each taxi type, 
    reporting averages, totals, and trip counts."""
    try:
        query = """
        SELECT 
            taxi_type,
            trip_hour,
            AVG(co2_per_trip_kg) as avg_co2_per_trip,
            SUM(co2_per_trip_kg) as total_co2,
            COUNT(*) as trip_count
        FROM emissions.main.taxi
        WHERE co2_per_trip_kg IS NOT NULL
        GROUP BY taxi_type, trip_hour
        ORDER BY taxi_type, avg_co2_per_trip DESC
        """
        
        results = con.execute(query).fetchall()
        logger.info("Retrieved hourly carbon analysis")
        return results
    except Exception as e:
        logger.error(f"Error getting hourly analysis: {e}")
        raise

def get_daily_carbon_analysis(con):
    """Summarizes CO2 emissions by day of week for each taxi type, 
    calculating average, total, and trip counts."""
    try:
        query = """
        SELECT 
            taxi_type,
            trip_day_of_week,
            AVG(co2_per_trip_kg) as avg_co2_per_trip,
            SUM(co2_per_trip_kg) as total_co2,
            COUNT(*) as trip_count
        FROM emissions.main.taxi
        WHERE co2_per_trip_kg IS NOT NULL
        GROUP BY taxi_type, trip_day_of_week
        ORDER BY taxi_type, avg_co2_per_trip DESC
        """
        
        results = con.execute(query).fetchall()
        logger.info("Retrieved daily carbon analysis")
        return results
    except Exception as e:
        logger.error(f"Error getting daily analysis: {e}")
        raise

def get_weekly_carbon_analysis(con):
    """Analyzes emissions by week of year for each taxi type, 
    computing average emissions, total CO2, and trip counts."""
    try:
        query = """
        SELECT 
            taxi_type,
            week_number,
            AVG(co2_per_trip_kg) as avg_co2_per_trip,
            SUM(co2_per_trip_kg) as total_co2,
            COUNT(*) as trip_count
        FROM emissions.main.taxi
        WHERE co2_per_trip_kg IS NOT NULL
        GROUP BY taxi_type, week_number
        ORDER BY taxi_type, avg_co2_per_trip DESC
        """
        
        results = con.execute(query).fetchall()
        logger.info("Retrieved weekly carbon analysis")
        return results
    except Exception as e:
        logger.error(f"Error getting weekly analysis: {e}")
        raise

def get_monthly_carbon_analysis(con):
    """Summarizes carbon emissions by month for each taxi type, 
    including average CO2, totals, and trip counts."""
    try:
        query = """
        SELECT 
            taxi_type,
            month,
            AVG(co2_per_trip_kg) as avg_co2_per_trip,
            SUM(co2_per_trip_kg) as total_co2,
            COUNT(*) as trip_count
        FROM emissions.main.taxi
        WHERE co2_per_trip_kg IS NOT NULL
        GROUP BY taxi_type, month
        ORDER BY taxi_type, avg_co2_per_trip DESC
        """
        
        results = con.execute(query).fetchall()
        logger.info("Retrieved monthly carbon analysis")
        return results
    except Exception as e:
        logger.error(f"Error getting monthly analysis: {e}")
        raise

def get_monthly_totals_for_plot(con):
    """Retrieves monthly CO2 totals by taxi type to support plotting 
    time-series emissions trends."""

    try:
        query = """
        SELECT 
            taxi_type,
            month,
            SUM(co2_per_trip_kg) as total_co2_kg
        FROM emissions.main.taxi
        WHERE co2_per_trip_kg IS NOT NULL
        GROUP BY taxi_type, month
        ORDER BY taxi_type, month
        """
        
        results = con.execute(query).fetchall()
        logger.info("Retrieved monthly totals for plotting")
        return results
    except Exception as e:
        logger.error(f"Error getting monthly totals: {e}")
        raise

def create_monthly_plot(monthly_data):
    """Builds and saves a line plot showing monthly CO2 totals by taxi type, 
    using Matplotlib for visualization."""
    try:
        # Convert to DataFrame for easier manipulation
        df = pd.DataFrame(monthly_data, columns=['taxi_type', 'month', 'total_co2_kg'])
        
        # Create the plot
        plt.figure(figsize=(12, 8))
        
        # Plot for each taxi type
        for taxi_type in df['taxi_type'].unique():
            type_data = df[df['taxi_type'] == taxi_type]
            plt.plot(type_data['month'], type_data['total_co2_kg'], 
                    marker='o', linewidth=2, label=f'{taxi_type.upper()} Taxi')
        
        plt.xlabel('Month (1-12)')
        plt.ylabel('Total CO2 (kg)')
        plt.title('Monthly CO2 Emissions by Taxi Type (2015-2024)')
        plt.legend()
        plt.grid(True, alpha=0.3)
        
        # Set month labels
        month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                      'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        plt.xticks(range(1, 13), month_names)
        
        # Save the plot
        plt.tight_layout()
        plt.savefig('monthly_co2_emissions.png', dpi=300, bbox_inches='tight')
        plt.close()
        
        logger.info("Created monthly CO2 emissions plot")
        print("Monthly CO2 emissions plot saved as 'monthly_co2_emissions.png'")
        
    except Exception as e:
        logger.error(f"Error creating plot: {e}")
        raise

def format_day_of_week(day_num):
    """Converts a numeric day (1–7) into its weekday name for readability."""

    days = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']
    return days[int(day_num) - 1] if 1 <= day_num <= 7 else f"Day {day_num}"

def format_month(month_num):
    """Converts a numeric month (1–12) into its month name for display purposes."""
    months = ['January', 'February', 'March', 'April', 'May', 'June',
              'July', 'August', 'September', 'October', 'November', 'December']
    return months[int(month_num) - 1] if 1 <= month_num <= 12 else f"Month {month_num}"

def print_analysis_results(con):
    """Runs all analysis queries, prints summaries of largest trips, hourly, daily, 
    weekly, and monthly patterns, and generates a monthly emissions plot."""
    try:
        print("=" * 80)
        print("NYC TAXI CARBON EMISSIONS ANALYSIS (2015-2024)")
        print("=" * 80)
        
        # 1. Largest carbon producing trips
        print("\n1. LARGEST CARBON PRODUCING TRIPS:")
        print("-" * 50)
        largest_trips = get_largest_carbon_trips(con)
        for trip in largest_trips:
            taxi_type, co2, distance, hour, day, week, month = trip
            print(f"{taxi_type.upper()} Taxi: {co2:.2f} kg CO2")
            print(f"  Distance: {distance:.2f} miles")
            print(f"  Time: {format_day_of_week(day)} at {hour}:00, Week {week}, {format_month(month)}")
            print()
        
        # 2. Hourly analysis
        print("\n2. HOURLY CARBON ANALYSIS:")
        print("-" * 50)
        hourly_data = get_hourly_carbon_analysis(con)
        
        for taxi_type in ['yellow', 'green']:
            type_data = [row for row in hourly_data if row[0] == taxi_type]
            if type_data:
                # Highest and lowest carbon hours
                highest = type_data[0]
                lowest = type_data[-1]
                print(f"{taxi_type.upper()} Taxi:")
                print(f"  Highest carbon hour: {highest[1]}:00 ({highest[2]:.3f} kg CO2 per trip)")
                print(f"  Lowest carbon hour: {lowest[1]}:00 ({lowest[2]:.3f} kg CO2 per trip)")
                print()
        
        # 3. Daily analysis
        print("\n3. DAILY CARBON ANALYSIS:")
        print("-" * 50)
        daily_data = get_daily_carbon_analysis(con)
        
        for taxi_type in ['yellow', 'green']:
            type_data = [row for row in daily_data if row[0] == taxi_type]
            if type_data:
                # Highest and lowest carbon days
                highest = type_data[0]
                lowest = type_data[-1]
                print(f"{taxi_type.upper()} Taxi:")
                print(f"  Highest carbon day: {format_day_of_week(highest[1])} ({highest[2]:.3f} kg CO2 per trip)")
                print(f"  Lowest carbon day: {format_day_of_week(lowest[1])} ({lowest[2]:.3f} kg CO2 per trip)")
                print()
        
        # 4. Weekly analysis
        print("\n4. WEEKLY CARBON ANALYSIS:")
        print("-" * 50)
        weekly_data = get_weekly_carbon_analysis(con)
        
        for taxi_type in ['yellow', 'green']:
            type_data = [row for row in weekly_data if row[0] == taxi_type]
            if type_data:
                # Highest and lowest carbon weeks
                highest = type_data[0]
                lowest = type_data[-1]
                print(f"{taxi_type.upper()} Taxi:")
                print(f"  Highest carbon week: Week {highest[1]} ({highest[2]:.3f} kg CO2 per trip)")
                print(f"  Lowest carbon week: Week {lowest[1]} ({lowest[2]:.3f} kg CO2 per trip)")
                print()
        
        # 5. Monthly analysis
        print("\n5. MONTHLY CARBON ANALYSIS:")
        print("-" * 50)
        monthly_data = get_monthly_carbon_analysis(con)
        
        for taxi_type in ['yellow', 'green']:
            type_data = [row for row in monthly_data if row[0] == taxi_type]
            if type_data:
                # Highest and lowest carbon months
                highest = type_data[0]
                lowest = type_data[-1]
                print(f"{taxi_type.upper()} Taxi:")
                print(f"  Highest carbon month: {format_month(highest[1])} ({highest[2]:.3f} kg CO2 per trip)")
                print(f"  Lowest carbon month: {format_month(lowest[1])} ({lowest[2]:.3f} kg CO2 per trip)")
                print()
        
        # 6. Create monthly plot
        print("\n6. CREATING MONTHLY CO2 EMISSIONS PLOT:")
        print("-" * 50)
        monthly_totals = get_monthly_totals_for_plot(con)
        create_monthly_plot(monthly_totals)
        
        logger.info("Analysis completed successfully")
        
    except Exception as e:
        logger.error(f"Error in analysis: {e}")
        raise

def main():
    """Coordinates the overall analysis workflow: connects to the database, 
    runs analysis, prints results, and ensures the connection is closed."""
    con = None
    try:
        logger.info("Starting carbon emissions analysis")
        print("Starting NYC Taxi Carbon Emissions Analysis...")
        
        # Connect to database
        con = get_connection()
        
        # Run analysis
        print_analysis_results(con)
        
        print("\nAnalysis completed successfully!")
        print("Check 'monthly_co2_emissions.png' for the visualization.")
        
    except Exception as e:
        logger.error(f"Analysis failed: {e}")
        print(f"Error: {e}")
        raise
    finally:
        if con:
            con.close()
            logger.info("Database connection closed")

if __name__ == "__main__":
    main()