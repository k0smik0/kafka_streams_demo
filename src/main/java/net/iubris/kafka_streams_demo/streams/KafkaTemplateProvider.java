package net.iubris.kafka_streams_demo.streams;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

/**
 * @author massimiliano.leone@capgemini.com
 *
 *         4 nov 2022
 *
 */
//@Log4j2
@Configuration
public class KafkaTemplateProvider {

	@Bean
	public ProducerFactory<String, Object> producerFactory() {
		Map<String, Object> configProps = new HashMap<>();
		configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, StreamsConfigurer.BROKERS);
		configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
		configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

		return new DefaultKafkaProducerFactory<>(configProps);
	}

	@Bean
	public <T> KafkaTemplate<String, T> kafkaTemplate(final ProducerFactory<String, T> producerFactory) {
		KafkaTemplate<String, T> kafkaTemplate = new KafkaTemplate<>(producerFactory);
		return kafkaTemplate;
	}

}
