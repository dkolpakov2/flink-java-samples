# flink-java-samples Template Docs
1. Key features of Flink:
 - True stream-first architecture
 - Support for event time and watermarks
 - Exactly-once semantics
 - Stateful and fault-tolerant
 - Integrates with Kafka, HDFS, S3, Cassandra, etc.
2. Difference between Flink and Spark Streaming:
Flink     					        |	Spark Streaming
---------------------------------------------------------------
Native stream processing    |  Micro-batch
Low-latency						      |  Higher latency
True event time support			|  Limited event time
Better fault tolerance			|  Less advanced state handling
---------------------------------------------------------------

3. Difference between processing time, event time, and ingestion time in Flink:
- Processing Time: Time on the machine running the job
- Event Time: Time when the event actually occurred
- Ingestion Time: When Flink first sees the event
✅ Event time + watermarks are preferred for accurate time-based analytics.
5. What is a Watermark in Flink?
A watermark is a mechanism to track event time progress in out-of-order streams. It tells Flink, “I don’t expect any events older
 than this timestamp.”
Used in : 
1. In-Order-Stream
2. Slightly Out of Order
3. Unpredictable Event Delays
DataStream<GenericRecord> attribDataStream = env
					.fromSource(attribsource, WatermarkStrategy.noWatermarks(), 
					"Kafka Source").setParallelism(1);
or 
DataStream<Event> stream = env
    .fromSource(source, 
        WatermarkStrategy
            .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5))
            .withTimestampAssigner((event, timestamp) -> event.getTimestamp()),
        "source-name");

6. Checkpointing in Flink:
  Checkpointing is Flink’s way of providing fault tolerance. It periodically saves the state of the       application, so it can resume from the last checkpoint after a failure.
7. Flink State:
  Flink allows you to store state inside your operators. State can be:
  Keyed state: Bound to a specific key
  Operator state: Bound to the operator, not keys
  Used heavily in windowing, aggregations, or custom logic.
8. Windowing in Flink
  Flink groups data over time using windows. Types:
  Tumbling: Fixed-size, non-overlapping
  Sliding: Fixed-size, with overlap
  Session: Based on inactivity gap
  data.keyBy(value -> value.userId)
    .window(TumblingEventTimeWindows.of(Time.minutes(5)))
    .reduce(...)
9. The consistency guarantees Flink provides:
  Exactly-once
  At-least-once (less expensive)
  With checkpoints and state backends like RocksDB, you can achieve exactly-once semantics.
10. How does Flink handle backpressure?
Flink uses async queues and watermarks to slow down upstream operators when downstream is slower, preventing memory overflow.
11. What are the different state backends in Flink?
MemoryStateBackend (local, dev)
FsStateBackend (stores metadata in memory, state in HDFS/S3)
RocksDBStateBackend (for large-scale stateful ops)
12. Flink jobs recover from failure:
  On failure:
    Flink restarts the job
    Restores operator state from the last successful checkpoint
    Reprocesses events from Kafka (or source)
13. Flink’s ProcessFunction explanation:
  It’s a low-level API for fine-grained control over processing, allowing:
  Custom timers
  Access to state
  Control over watermarks
14. ## Java example:
  public class MyProcessFunction extends ProcessFunction<String, String> {
    public void processElement(...) {
      // custom logic here
    }
  }

15. Behavioral/Design:  design a Flink pipeline to process real-time user clickstream data?
  about: Kafka source
    Deserialization
    Event-time processing
    Aggregations (e.g. session duration)
    Sinks like Elasticsearch, S3, or JDBC
16. When a Flink is been preferred over Kafka Streams or Spark Structured Streaming:
  Need low latency (<100ms)
  Complex event-time logic
  Large stateful streaming jobs
  Scalability with fault tolerance
