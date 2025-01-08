from spark_configurations import create_basic_session
import os
from enum import Enum
from pyspark.sql import SparkSession

"""
Basic Spark Session

def create_basic_session() -> SparkSession:
    return SparkSession.builder.appName("Basic Configuration") \
                               .master(local[*]) \
                               .getorcreate()

To remove the warning message, we can set the log level to ERROR:
For the issue of the hostname resolution, we can set the hostname to localhost:
"""



class SparkLogLevel(Enum):
    """Available Spark log levels"""
    ALL = "ALL"
    DEBUG = "DEBUG"
    ERROR = "ERROR"
    FATAL = "FATAL"
    INFO = "INFO"
    OFF = "OFF"
    TRACE = "TRACE"
    WARN = "WARN"

class SparkSessionManager:
    "Spark Session Manager class"
    def __init__(self, app_name: str = "Configure Spark Environment"):
        self.app_name = app_name
        self.spark = configure_spark_environment()

    def set_log_level(self, level: SparkLogLevel):
        self.spark.sparkContext.setLogLevel(level.value)

    def get_log_level(self) -> str:
        return self.spark.sparkContext._jsc.sc().toString()

    def stop(self) -> None:
        if self.spark:
            self.spark.stop()

""""
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
25/01/07 15:57:50 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable


To resolve the Above issue we can use these three spark session configurations:
.config("spark.driver.host", "192.168.0.106") \
.config("spark.driver.bindAddress", "192.168.0.106") \
.config("spark.hadoop.io.compression.codecs",
"org.apache.hadoop.io.compress.SnappyCodec") \

Configure the master host name and also bind the address to show the status of the driver and executor.
for the warning of the native-hadoop library, we can set the compression codec to SnappyCodec.
"""

def configure_spark_environment(log_level: str = SparkLogLevel.ERROR.value):
    """Configure Spark environment with custom log level
    Args:
        log_level: Logging level (default: ERROR)
    Returns:
        SparkSession: Configured spark session
    """
    # SET THE ENVIRONMENT VARIABLE BEFORE CREATING SESSION
    os.environ["Spark_LOCAL_IP"] = "localhost"

    # CREATE THE SPARK SESSION
    spark: any = create_basic_session()

    # set the log level to ERROR to minimize the log messages
    spark.sparkContext.setLogLevel("ERROR")

    return spark

def create_session() -> SparkSession:
    """Create a Spark session with custom configuration to fix the warning message"""
    return SparkSession.builder \
        .appName("Basic Configuration") \
        .master("local[*]") \
        .config("spark.driver.host", "192.168.0.106") \
        .config("spark.driver.bindAddress", "192.168.0.106") \
        .config("spark.hadoop.io.compression.codecs",
                "org.apache.hadoop.io.compress.SnappyCodec") \
        .getOrCreate()


if __name__ == "__main__":
    spark: any = configure_spark_environment()
    print(spark)
    spark.stop()
    print("Session first stopped")

    spark: any = create_session()
    print("Second Spark Session")
    print(spark)
    spark.stop()
    print("Second Session stopped")

    try:
        manager = SparkSessionManager()
        manager.set_log_level(SparkLogLevel.ERROR)
        print(f"Current log level: {manager.get_log_level()}")
    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        manager.stop()
