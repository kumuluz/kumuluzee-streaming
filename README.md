# KumuluzEE Event Streaming
[![Build Status](https://img.shields.io/travis/kumuluz/kumuluzee-streaming/master.svg?style=flat)](https://travis-ci.org/kumuluz/kumuluzee-streaming)

> KumuluzEE Event Streaming project for developing event-based microservices using Apache Kafka.

KumuluzEE Event Streaming project for the KumuluzEE microservice framework provides easy-to-use annotations for
developing microservices that produce or consume event streams. KumuluzEE Event Streaming has been designed to support
modularity with pluggable streaming platforms. Currently, [Apache Kafka](https://kafka.apache.org/) is supported. In
the future, other event streaming platforms will be supported too (contributions are welcome).

## Usage

You can enable KumuluzEE Event Streaming with Kafka by adding the following dependency:

```xml
<dependency>
    <groupId>com.kumuluz.ee.streaming</groupId>
    <artifactId>kumuluzee-streaming-kafka</artifactId>
    <version>${kumuluzee-streaming.version}</version>
</dependency>
```

If you would like to collect Kafka related logs through the KumuluzEE Logs, you have to include the `kumuluzee-logs`
implementation and slf4j-log4j adapter dependencies:

```xml
<dependency>
    <artifactId>kumuluzee-logs-log4j2</artifactId>
    <groupId>com.kumuluz.ee.logs</groupId>
    <version>${kumuluzee-logs.version}</version>
</dependency>

<dependency>
    <groupId>org.apache.logging.log4j</groupId>
    <artifactId>log4j-slf4j-impl</artifactId>
    <version>${log4j-slf4j-impl.version}</version>
</dependency>
```

You also need to include a Log4j2 configuration, which should be in a file named `log4j2.xml`, located in
`src/main/resources`. For more information about KumuluzEE Logs visit the
[KumuluzEE Logs Github page](https://github.com/kumuluz/kumuluzee-logs).

#### Configuring Kafka Producers and Consumers

Kafka Consumers and Producers are configured with the common KumuluzEE configuration framework. Configuration properties
can be defined with the environment variables or with the configuration files. Alternatively, they can also be stored in
a configuration server, such as etcd or Consul (for which the KumuluzEE Config project is required). For more details
see the [KumuluzEE configuration wiki page](https://github.com/kumuluz/kumuluzee/wiki/Configuration) and
[KumuluzEE Config](https://github.com/kumuluz/kumuluzee-config).
The default configuration prefix for consumers is `consumer`, for producers is `producer`, but you can assign your
custom configuration prefix. This way you can configure several different producers and/or consumers at the same time.

The example below shows a sample configuration for the Kafka producer and consumer using default prefix.

```yaml
# producer config
kumuluzee:
  streaming:
    kafka:
      producer:
        bootstrap-servers: localhost:9092
        acks: all
        retries: 0
        batch-size: 16384
        linger-ms: 1
        buffer-memory: 33554432
        key-serializer: org.apache.kafka.common.serialization.StringSerializer
        value-serializer: org.apache.kafka.common.serialization.StringSerializer
        . . .

# consumer config
kumuluzee:
  streaming:
    kafka:
      consumer:
        bootstrap-servers: localhost:9092
        group-id: group1
        enable-auto-commit: true
        auto-commit-interval-ms: 1000
        auto-offset-reset: earliest
        key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
        value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
        . . .
```

To use custom prefix, the configuration could look like this:

```yaml
# custom producer config
kumuluzee:
  streaming:
    kafka:
      custom-producer:
        bootstrap-servers: localhost:9092
        acks: all
        retries: 0
        batch-size: 16384
        linger-ms: 1
        buffer-memory: 33554432
        key-serializer: org.apache.kafka.common.serialization.StringSerializer
        value-serializer: org.apache.kafka.common.serialization.StringSerializer
        . . .

# custom consumer config
kumuluzee:
  streaming:
    kafka:
      custom-consumer:
        bootstrap-servers: localhost:9092
        group-id: group1
        enable-auto-commit: true
        auto-commit-interval-ms: 1000
        auto-offset-reset: earliest
        key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
        value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
        . . .
```

You can also configure the Kafka Consumer poll parameter timeout, which is the time, in milliseconds, spent waiting in
poll if data is not available in the buffer. If 0, it returns immediately with any records that are available currently
in the buffer. Otherwise it returns empty. Must not be negative. In the KumuluzEE configuration you define the timeout
parameter like this:

```yaml
kumuluzee:
  streaming:
    kafka:
      poll-timeout: 1000
```

### Stream Producer annotation

For injecting the Kafka `Producer`, the KumuluzEE Kafka provides a `@StreamProducer` annotation, which will inject the
producer reference. We have to use it in conjunction with the `@Inject` annotation, as shown on the example below.  
The example bellow shows an example `@StreamProducer` code excerpt:

```java
@Inject
@StreamProducer
private Producer<String, String> producer;
``` 

The annotation has two parameter, both of which are optional. The `config` parameter is used for assigning the custom
producer configuration prefix, used in the KumuluzEE configuration. If not specified, the default value is `producer`.
The next example shows how to specify a custom producer configuration prefix within the annotation:

```java
@Inject
@StreamProducer(config = "custom-producer")
private Producer<String, String> producer;
```

The second parameter is `configOverrides` and is covered in the section _Overriding configuration_ below.

### Stream Consumer annotation

For consuming Kafka messages, KumuluzEE Event Streaming with Kafka provides the `@StreamListener` annotation. It is
used to annotate the method that will be invoked when a message is received. It works similarly as a classic JMS
listener or a MDB. Please pay attention to the fact that you can only use application scoped beans in the
`@StreamListener` annotated method.

The annotation takes four parameters: 

- `topics` an array of topics names, if none is defined the name of the annotated method will be used as a topic name. 
- `config` is the configuration prefix name for the KumuluzEE configuration. The default value is `consumer`.
- `batchListener` a boolean value, for enabling batch message consuming. The default value is `false`.
- `configOverrides` covered in the section _overriding configuration_ below. The default value is empty array (`{}`).

The example shows a `@StreamListener` annotated _topicName_ method with default configuration prefix name:

```java
@StreamListener
public void topicName(ConsumerRecord<String, String> record) {
	// process the message record
}
``` 

If you like to add custom configuration prefix name and specify topic names in the annotation, you can do it like this:

```java
@StreamListener(topics = {"topic1", "topic2"}, config = "custom-consumer")
public void onMessage(ConsumerRecord<String, String> record) {
	// process the message record
}
``` 

You can also consume a batch of messages, with the `batchListener` parameter set to `true`. In this case the annotated
method parameter must be a List of ConsumerRecords, like in the example below:

```java
@StreamListener(topics = {"topic"}, batchListener = true)
public void onMessage(List<ConsumerRecord<String, String>> records) {
	// process the message records
}
``` 

The `@StreamListener` annotation also allows manual message committing. First you have to set the property of 
`enable.auto.commit` in the consumer configuration to `false`. Then add another parameter `Acknowledgement` to the 
annotated method, which has two methods for committing the message offsets:

- `acknowledge()` that commits the last consumed message for all the subscribed list of topics and partitions and
- `acknowledge(java.util.Map<TopicPartition,OffsetAndMetadata> offsets)` that commits the specified offsets for the 
  specified list of topics and partitions

Example of manual message committing:

```java
@StreamListener(topics = {"topic"})
public void onMessage(ConsumerRecord<String, String> record, Acknowledgement ack) {
	// process the message record
	
	// commit the message record
	ack.acknowledge();
}
```

### Stream processing

KumuluzEE Event Streaming with Kafka supports stream processors. `@StreamProcessor` annotation is used for building a
stream processor.

Example of stream processor:

```java
@StreamProcessor(id = "word-count", autoStart = false)
public StreamsBuilder wordCountBuilder() {

    StreamsBuilder builder = new StreamsBuilder();

    // configure the builder

    return builder;

}
```

`@StreamProcessor` annotation has several parameters. The `config` parameter specifies the prefix used for configuration
lookup, similar to the one used by `@StreamProducer` and `@StreamListener` annotations described above. The `autoStart`
parameter allows automatic or manual initiation of the stream processor.

If the `autoStart` parameter is set to `false`, a `StreamsController` can be used to control the lifecycle of
the stream processor. `@StreamProcessorController` is used to obtain an instance of `StreamsController`.

Example usage of `StreamsController`:

```java
@StreamProcessorController(id="word-count")
StreamsController wordCountStreams;

public void startStream(@Observes @Initialized(ApplicationScoped.class) Object init) {
    wordCountStreams.start();
}
```

### Overriding configuration

The annotations `@StreamProducer`, `@StreamListener` and `@StreamProcessor` support the parameter `configOverrides`,
which enables user to override or supply additional configuration from the code. For example:

```java
@Inject
@StreamProducer(configOverrides = {@ConfigurationOverride(key = "bootstrap-servers", value = "localhost:1234")})
private Producer<String, String> overriddenProducer;
```

## Changelog

Recent changes can be viewed on Github on the [Releases Page](https://github.com/kumuluz/kumuluzee-streaming/releases)

## Contribute

See the [contributing docs](https://github.com/kumuluz/kumuluzee-streaming/blob/master/CONTRIBUTING.md)

When submitting an issue, please follow the 
[guidelines](https://github.com/kumuluz/kumuluzee-streaming/blob/master/CONTRIBUTING.md#bugs).

When submitting a bugfix, write a test that exposes the bug and fails before applying your fix. Submit the test 
alongside the fix.

When submitting a new feature, add tests that cover the feature.

## License

MIT
