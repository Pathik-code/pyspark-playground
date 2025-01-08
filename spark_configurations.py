from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

def create_basic_session():
    """Basic Spark Session"""
    return SparkSession.builder \
        .appName("Basic Configuration") \
        .master("local[*]") \
        .getOrCreate()

"""
If we have to use all the cores of the system then use local[*]
else we can specify the number of cores to be used by using local[n]
-- In the mentioned number of cores we have to match the parallelism with cores
"""


"""
### Spark Serializers Explanation

#### Plan:
1. Define serialization in Spark context
2. List available serializers
3. Compare Java vs Kryo serializer
4. Show configuration examples

### What is Serialization?
- Process of converting objects into byte streams for:
  - Network transmission
  - Disk storage
  - Cache storage
  - Data shuffling

### Types of Serializers in Spark:

1. **Java Serializer (Default)**
   - Built-in Java serialization
   - More generic but slower
   - Default in Spark

2. **Kryo Serializer**
   - Faster (up to 10x)
   - More compact serialization
   - Better for primitive types
   - Requires class registration for best performance

### Example Configuration:

```python


def create_configured_session():
    # Spark Session with detailed configurations
    conf = SparkConf() \
        .setAppName("Serialization Example") \
        .setMaster("local[*]") \
        # Use Kryo serializer
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        # Register custom classes (optional)
        .set("spark.kryo.registrationRequired", "false") \
        # Increase buffer size for better performance
        .set("spark.kryoserializer.buffer.max", "1g") \
        # Register specific classes (optional)
        .set("spark.kryo.classesToRegister",
             "org.apache.spark.examples.MyClass1,org.apache.spark.examples.MyClass2")

    return SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()
```

### Use Cases:
- Kryo: Large-scale data processing, performance-critical applications
- Java: Debug mode, when compatibility is priority
"""

"""
### Off-Heap Memory in Spark: Explanation

#### What is Off-Heap Memory?
1. Memory management approach where data is stored outside JVM heap
2. Managed by OS/native memory space
3. Bypasses Java garbage collection

#### Benefits:
- Reduces GC overhead
- Better memory management
- Improved performance for large datasets

```python


def create_configured_session():
    conf = SparkConf() \
        .setAppName("Memory Configuration") \
        # Enable off-heap memory
        .set("spark.memory.offHeap.enabled", "true") \
        # Set off-heap memory size (1GB)
        .set("spark.memory.offHeap.size", "1g") \
        # Set on-heap memory
        .set("spark.executor.memory", "2g") \
        # Additional memory safety
        .set("spark.memory.fraction", "0.6") \
        .set("spark.memory.storageFraction", "0.5")

    return SparkSession.builder.config(conf=conf).getOrCreate()
```

#### Memory Types:
1. **On-Heap Memory**
   - Managed by JVM
   - Subject to garbage collection
   - Limited by JVM heap size

2. **Off-Heap Memory**
   - Managed outside JVM
   - No garbage collection impact
   - Direct memory access
   - Better for large, long-lived objects
"""

def create_configured_session(num_of_cores=2):
    """Spark Session with detailed configurations"""
    conf = SparkConf() \
        .setAppName("Advanced Configuration") \
        .setMaster(f"local[{num_of_cores}]") \
        .set("spark.executor.memory", "2g") \
        .set("spark.driver.memory", "2g") \
        .set("spark.memory.offHeap.enabled", "true") \
        .set("spark.memory.offHeap.size", "1g") \
        .set("spark.sql.shuffle.partitions", "100") \
        .set("spark.default.parallelism", f"{num_of_cores}") \
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .set("spark.driver.maxResultSize", "1g")

    return SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

def create_session_with_packages():
    """Spark Session with external packages"""
    return SparkSession.builder \
        .appName("Packages Configuration") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
        .getOrCreate()

def create_hive_session():
    """Spark Session with Hive support"""
    return SparkSession.builder \
        .appName("Hive Configuration") \
        .config("spark.sql.warehouse.dir", "/path/to/hive/warehouse") \
        .enableHiveSupport() \
        .getOrCreate()

def create_spark_context():
    """Create SparkContext with detailed configurations"""
    conf = SparkConf() \
        .setAppName("SparkContext Configuration") \
        .setMaster("local[*]") \
        .set("spark.executor.memory", "2g") \
        .set("spark.driver.memory", "2g") \
        .set("spark.executor.cores", "2") \
        .set("spark.driver.cores", "2") \
        .set("spark.executor.instances", "2") \
        .set("spark.default.parallelism", "4") \
        .set("spark.network.timeout", "800s") \
        .set("spark.executor.heartbeatInterval", "60s")

    sc = SparkContext(conf=conf)
    return sc

def create_sql_context(spark_context):
    """Create SQLContext with configurations"""
    sql_context = SQLContext(spark_context)
    sql_context.setConf("spark.sql.shuffle.partitions", "100")
    sql_context.setConf("spark.sql.autoBroadcastJoinThreshold", "10485760")  # 10MB
    sql_context.setConf("spark.sql.files.maxPartitionBytes", "134217728")    # 128MB
    return sql_context

def show_available_configurations(spark):
    """Display current Spark configurations"""
    print("\nCurrent Spark Configurations:")
    for item in spark.sparkContext.getConf().getAll():
        print(f"{item[0]}: {item[1]}")

def main():
    # 1. Basic Configuration
    print("\n=== Basic Configuration ===")
    basic_spark = create_basic_session()
    show_available_configurations(basic_spark)
    basic_spark.stop()

    # 2. Advanced Configuration
    print("\n=== Advanced Configuration ===")
    configured_spark = create_configured_session()
    show_available_configurations(configured_spark)
    configured_spark.stop()

    # 3. SparkContext Configuration
    print("\n=== SparkContext Configuration ===")
    sc = create_spark_context()
    print("\nSparkContext Configurations:")
    for item in sc.getConf().getAll():
        print(f"{item[0]}: {item[1]}")

    # 4. SQLContext Configuration
    print("\n=== SQLContext Configuration ===")
    sql_context = create_sql_context(sc)
    print("\nSQLContext Configurations:")
    print(f"shuffle.partitions: {sql_context.getConf('spark.sql.shuffle.partitions')}")
    print(f"autoBroadcastJoinThreshold: {sql_context.getConf('spark.sql.autoBroadcastJoinThreshold')}")
    print(f"maxPartitionBytes: {sql_context.getConf('spark.sql.files.maxPartitionBytes')}")

    sc.stop()

    # Common Configuration Properties Guide
    print("\n=== Common Spark Configuration Properties ===")
    config_guide = {
        "Application Properties": {
            "spark.app.name": "Sets application name",
            "spark.driver.cores": "Number of cores for driver",
            "spark.driver.memory": "Memory for driver",
            "spark.executor.memory": "Memory per executor"
        },
        "Runtime Properties": {
            "spark.python.worker.memory": "Python worker memory",
            "spark.default.parallelism": "Default number of partitions",
            "spark.sql.shuffle.partitions": "Number of partitions for shuffling"
        },
        "Shuffle Properties": {
            "spark.shuffle.file.buffer": "Size of shuffle file buffer",
            "spark.shuffle.spill.compress": "Compress shuffle spill files"
        },
        "Compression and Serialization": {
            "spark.serializer": "Class for serializing objects",
            "spark.rdd.compress": "Compress serialized RDD partitions"
        },
        "Memory Management": {
            "spark.memory.fraction": "Fraction of heap for execution and storage",
            "spark.memory.storageFraction": "Amount of storage memory from spark.memory.fraction"
        },
        "Security Properties": {
            "spark.authenticate": "Enable authentication",
            "spark.network.crypto.enabled": "Enable encryption"
        },
        "SparkContext Specific": {
            "spark.executor.cores": "Number of cores per executor",
            "spark.executor.instances": "Number of executors",
            "spark.network.timeout": "Default network timeout",
            "spark.executor.heartbeatInterval": "Executor heartbeat interval"
        },
        "SQLContext Specific": {
            "spark.sql.shuffle.partitions": "Number of partitions for shuffling",
            "spark.sql.autoBroadcastJoinThreshold": "Maximum size for broadcast joins",
            "spark.sql.files.maxPartitionBytes": "Maximum partition size for reading files"
        }
    }

    for category, properties in config_guide.items():
        print(f"\n{category}:")
        for prop, description in properties.items():
            print(f"  {prop}: {description}")

if __name__ == "__main__":
    main()
