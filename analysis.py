import duckdb
import logging
import os

os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename=os.path.join("logs", "analysis.log"),
    filemode="a"
)
logger = logging.getLogger(__name__)

def get_connection(db_path: str = "emissions.duckdb"):
    """Establish DuckDB connection with error handling."""
    try:
        con = duckdb.connect(db_path)
        logger.info(f"Connected to DuckDB: {db_path}")
        return con
    except Exception as e:
        logger.error(f"Connection failed: {e}")
        raise
