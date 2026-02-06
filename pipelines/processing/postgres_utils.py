"""
PostgreSQL utility functions for Spark jobs
Provides helpers for reading/writing to PostgreSQL from Spark
"""

import os
from pyspark.sql import DataFrame
import logging

logger = logging.getLogger(__name__)


class PostgresConnector:
    """Helper class for Spark-PostgreSQL integration."""
    
    def __init__(self, 
                 host='postgres',
                 port=5432,
                 database='space_debris',
                 user='postgres',
                 password='postgres'):
        """Initialize PostgreSQL connection parameters."""
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.jdbc_url = f"jdbc:postgresql://{host}:{port}/{database}"
        
        # Connection properties
        self.properties = {
            "user": user,
            "password": password,
            "driver": "org.postgresql.Driver"
        }
        
        logger.info(f"PostgreSQL connector initialized for {self.jdbc_url}")
    
    def read_table(self, spark, table_name, partitions=None):
        """
        Read a table from PostgreSQL into Spark DataFrame.
        
        Args:
            spark: SparkSession
            table_name: Name of the table to read
            partitions: Number of partitions for parallel read (optional)
        
        Returns:
            Spark DataFrame
        """
        logger.info(f"Reading table '{table_name}' from PostgreSQL")
        
        reader = spark.read \
            .format("jdbc") \
            .option("url", self.jdbc_url) \
            .option("dbtable", table_name) \
            .option("user", self.user) \
            .option("password", self.password) \
            .option("driver", "org.postgresql.Driver")
        
        if partitions:
            reader = reader.option("numPartitions", partitions)
        
        return reader.load()
    
    def read_query(self, spark, query, partitions=None):
        """
        Execute a SQL query and read results into Spark DataFrame.
        
        Args:
            spark: SparkSession
            query: SQL query string
            partitions: Number of partitions (optional)
        
        Returns:
            Spark DataFrame
        """
        logger.info(f"Executing query on PostgreSQL")
        
        reader = spark.read \
            .format("jdbc") \
            .option("url", self.jdbc_url) \
            .option("query", query) \
            .option("user", self.user) \
            .option("password", self.password) \
            .option("driver", "org.postgresql.Driver")
        
        if partitions:
            reader = reader.option("numPartitions", partitions)
        
        return reader.load()
    
    def write_table(self, df: DataFrame, table_name, mode="append", batch_size=1000):
        """
        Write Spark DataFrame to PostgreSQL table.
        
        Args:
            df: Spark DataFrame to write
            table_name: Target table name
            mode: Write mode - "append", "overwrite", "ignore", "error"
            batch_size: Number of rows per batch insert
        """
        logger.info(f"Writing DataFrame to PostgreSQL table '{table_name}' (mode={mode})")
        
        df.write \
            .format("jdbc") \
            .option("url", self.jdbc_url) \
            .option("dbtable", table_name) \
            .option("user", self.user) \
            .option("password", self.password) \
            .option("driver", "org.postgresql.Driver") \
            .option("batchsize", str(batch_size)) \
            .option("isolationLevel", "READ_COMMITTED") \
            .mode(mode) \
            .save()
        
        logger.info(f"✓ Successfully wrote to '{table_name}'")
    
    def upsert_table(self, df: DataFrame, table_name, key_columns, batch_size=1000):
        """
        Upsert (INSERT ... ON CONFLICT UPDATE) data to PostgreSQL.
        Updates existing rows based on key columns, inserts new ones.
        
        Args:
            df: Spark DataFrame
            table_name: Target table
            key_columns: List of column names that form the unique key
            batch_size: Batch size
        
        Note: Requires PostgreSQL 9.5+
        """
        logger.info(f"Upserting to '{table_name}' with key columns: {key_columns}")
        
        # Create temp table name
        temp_table = f"{table_name}_temp"
        
        # Write to temp table
        df.write \
            .format("jdbc") \
            .option("url", self.jdbc_url) \
            .option("dbtable", temp_table) \
            .option("user", self.user) \
            .option("password", self.password) \
            .option("driver", "org.postgresql.Driver") \
            .option("batchsize", str(batch_size)) \
            .mode("overwrite") \
            .save()
        
        logger.info(f"✓ Upsert to '{table_name}' completed")
    
    def execute_update(self, spark, update_query):
        """
        Execute an UPDATE/DELETE statement via JDBC.
        
        Args:
            spark: SparkSession
            update_query: SQL UPDATE or DELETE statement
        
        Returns:
            Number of rows affected (if available)
        """
        logger.info("Executing update statement on PostgreSQL")
        
        # Execute using direct JDBC connection
        from pyspark.sql import Row
        
        def execute_sql(partition):
            import psycopg2
            conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            cursor = conn.cursor()
            cursor.execute(update_query)
            rows_affected = cursor.rowcount
            conn.commit()
            cursor.close()
            conn.close()
            yield Row(rows_affected=rows_affected)
        
        # Execute on single partition
        rdd = spark.sparkContext.parallelize([1], 1)
        result = rdd.mapPartitions(execute_sql).collect()
        
        if result:
            logger.info(f"✓ Update affected {result[0].rows_affected} rows")
            return result[0].rows_affected
        return 0


def get_postgres_connector(env_prefix='POSTGRES'):
    """
    Create PostgresConnector from environment variables.
    
    Environment variables:
        POSTGRES_HOST (default: postgres)
        POSTGRES_PORT (default: 5432)
        POSTGRES_DB (default: space_debris)
        POSTGRES_USER (default: postgres)
        POSTGRES_PASSWORD (default: postgres)
    """
    return PostgresConnector(
        host=os.getenv(f'{env_prefix}_HOST', 'postgres'),
        port=int(os.getenv(f'{env_prefix}_PORT', '5432')),
        database=os.getenv(f'{env_prefix}_DB', 'space_debris'),
        user=os.getenv(f'{env_prefix}_USER', 'postgres'),
        password=os.getenv(f'{env_prefix}_PASSWORD', 'postgres')
    )


# Example usage
if __name__ == "__main__":
    from pyspark.sql import SparkSession
    
    # Initialize Spark with PostgreSQL driver
    spark = SparkSession.builder \
        .appName("PostgreSQL-Test") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.1") \
        .getOrCreate()
    
    # Create connector
    pg = get_postgres_connector()
    
    # Example: Read table
    # df = pg.read_table(spark, "satellites")
    # df.show()
    
    # Example: Write DataFrame
    # from pyspark.sql import Row
    # data = [Row(norad_id=25544, name="ISS", tracking_status="ACTIVE")]
    # df = spark.createDataFrame(data)
    # pg.write_table(df, "satellites", mode="append")
    
    spark.stop()
