package net.iubris.kafka_streams_demo.streams;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

import lombok.extern.log4j.Log4j2;

import jakarta.annotation.PostConstruct;
import net.iubris.kafka_streams_demo.model.AggregateValue;
import net.iubris.kafka_streams_demo.model.MessageValue;

/**
 * @author massimiliano.leone@capgemini.com
 *
 *         20 set 2023
 *
 */
@Log4j2
@Component
public class StreamsConfigurer {

	public static final String BROKERS = "10.24.160.62:9193";

	public static final String TOPIC_PREFIX = "biaw_input_";

//	private static final org.apache.logging.log4j.Logger log = org.apache.logging.log4j.LogManager.getLogger(StreamsConfigurer.class);
//	private static final Logger log = LogManager.getLogger();

	private static final Predicate<String, AggregateValue> IS_AGGREGATION_COMPLETED = (key, value) -> {
		boolean completed = value.getCompletedAt() == value.getPayload().size();
		return completed;
	};

	@Value("${spring.application.name}")
	protected String applicationName;

	private Topology buildTopology() {
		StreamsBuilder streamsBuilder = new StreamsBuilder();

		KGroupedStream<String, MessageValue> input1GroupedByKey = streamsBuilder
			// listen on topic
			.stream(TOPIC_PREFIX + "1", Consumed.with(serdeForKey, serdeForInputValue))
			.peek((key, value) -> log.info("input1:: key:{}, value:{}", () -> key, () -> value))
			// tell to group messages by key
			.groupByKey();

		KGroupedStream<String, MessageValue> input2GroupedByKey = streamsBuilder
			.stream(TOPIC_PREFIX + "2", Consumed.with(serdeForKey, serdeForInputValue))
			.peek((key, value) -> log.info("input2:: key:{}, value:{}", () -> key, () -> value))
			.groupByKey();
		KGroupedStream<String, MessageValue> input3GroupedByKey = streamsBuilder
			.stream(TOPIC_PREFIX + "3", Consumed.with(serdeForKey, serdeForInputValue))
			.peek((key, value) -> log.info("input3:: key:{}, value:{}", () -> key, () -> value))
			.groupByKey();

		final AggregatorProcessor aggregatorProcessor = new AggregatorProcessor();
		final Initializer<AggregateValue> aggregatorInitializer = () -> {
			AggregateValue aggregateValue = new AggregateValue();
			return aggregateValue;
		};

		input1GroupedByKey
			.cogroup(aggregatorProcessor)
			.cogroup(input2GroupedByKey, aggregatorProcessor)
			.cogroup(input3GroupedByKey, aggregatorProcessor)
			.aggregate(aggregatorInitializer,
					Named.as("biaw_intermediate_aggregation"),
					buildStateStore("biaw_intermediate_aggregation__table", serdeForKey, serdeForAggregateValue))
			// from here: no more statefulness by changelog, but we store in KTable so we should have an enough safeness place
			.filter(IS_AGGREGATION_COMPLETED,
					Named.as("biaw_completed_aggregation"),
					buildStateStore("biaw_completed_aggregation__table", serdeForKey, serdeForAggregateValue))
			// from here: data to send to output when aggregation completes
			.toStream(Named.as("biaw_output"))
			.peek((key, aggregated) -> log.info("Aggregated:: key:{}, value:{}", () -> key, () -> aggregated));

		return streamsBuilder.build();
	}

	public static class AggregatorProcessor implements Aggregator<String, MessageValue, AggregateValue> {
		@Override
		public AggregateValue apply(final String key, final MessageValue value, final AggregateValue aggregate) {
			aggregate.getPayload().put(value.getMessageIndex(), "" + System.currentTimeMillis());
			aggregate.setCompletedAt(value.getToCompleteAt());
			return aggregate;
		}
	}

	private static <V> Materialized<String, V, KeyValueStore<Bytes, byte[]>> buildStateStore(
			final String changelogTopicName,
			@SuppressWarnings("hiding") final Serde<String> serdeForKey,
			final Serde<V> serdeForValue) {
		Duration stateRetention = Duration.ofDays(3);

		Materialized<String, V, KeyValueStore<Bytes, byte[]>> store = Materialized
			.<String, V, KeyValueStore<Bytes, byte[]>>as(changelogTopicName)
			.withRetention(stateRetention)
			.withKeySerde(serdeForKey)
			.withValueSerde(serdeForValue);

		return store;
	}

	public static final Serde<String> serdeForKey = buildJSONSerdeForKey();
	public static final Serde<MessageValue> serdeForInputValue = buildJSONSerdeForValue(MessageValue.class);
	public static final Serde<AggregateValue> serdeForAggregateValue = buildJSONSerdeForValue(AggregateValue.class);

	@PostConstruct
	public void init() {
		Properties config = buildConfig();
		StreamsConfig streamsConfig = new StreamsConfig(config);
		Topology topology = buildTopology();
		log.info("{} topology: {}", () -> applicationName, topology::describe);
		@SuppressWarnings("resource")
		KafkaStreams kafkaStreams = new KafkaStreams(topology, streamsConfig);
		kafkaStreams.start();
	}

	private Properties buildConfig() {
		Properties config = new Properties();
		config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
		// TODO use also JsonSerde for default_key_serde ?
		config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class.getName());
		config.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class.getName());
		config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
		config.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 3);
		config.put(StreamsConfig.TASK_TIMEOUT_MS_CONFIG, 7200);
		config.put(StreamsConfig.POLL_MS_CONFIG, 100);

		config.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationName);

		return config;
	}

	protected static Serde<String> buildJSONSerdeForKey() {
		Map<String, Object> props = new HashMap<>();
		props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
		props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, "false");
		props.put(JsonDeserializer.KEY_DEFAULT_TYPE, String.class);
		JsonSerde<String> jsonSerde = new JsonSerde<>(String.class);
		// true: is for key
		jsonSerde.configure(props, true);
		return jsonSerde;
	}

	public static <T> Serde<T> buildJSONSerdeForValue(final Class<T> clazz) {
		Map<String, Object> props = new HashMap<>();
		props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
		props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, "false");
		// props.put(JsonSerializer.TYPE_MAPPINGS, "false");
		// use string, despite it seems boolean
		// props.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);
		// props.put(JsonDeserializer.KEY_DEFAULT_TYPE, String.class);
		props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, clazz);
		// final JsonSerializer<T> jsonSerializer = new JsonSerializer<>();
		// final JsonDeserializer<T> jsonDeserializer = new JsonDeserializer<>(clazz);
		// final Serde<T> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
		JsonSerde<T> jsonSerde = new JsonSerde<>(clazz);
		// false: is not for key
		jsonSerde.configure(props, false);

		return jsonSerde;
	}

}
