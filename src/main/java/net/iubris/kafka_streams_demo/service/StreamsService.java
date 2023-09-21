package net.iubris.kafka_streams_demo.service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import lombok.extern.log4j.Log4j2;

import net.iubris.kafka_streams_demo.controller.StreamsController.TestRequest;
import net.iubris.kafka_streams_demo.controller.StreamsController.TestResponse;
import net.iubris.kafka_streams_demo.model.MessageValue;
import net.iubris.kafka_streams_demo.streams.StreamsConfigurer;

/**
 * @author massimiliano.leone@capgemini.com
 *
 *         20 set 2023
 *
 */
@Log4j2
@Service
public class StreamsService {

	@Autowired
	private KafkaTemplate<String, MessageValue> kafkaTemplate;

	public TestResponse execute(final TestRequest request) throws Exception {

		String now = DATETIME_FORMATTER_yyyyMMddHHmmss.format(LocalDateTime.now());
		log.info("Begin sending {} messages for key:{} ...", () -> request.getToCompleteAt(), () -> now);
		CompletableFuture.supplyAsync(() -> {
			ThreadLocalRandom current = ThreadLocalRandom.current();
			IntStream
				.rangeClosed(1, request.getToCompleteAt())
				.boxed()
				.parallel()
				.forEach(i -> {
					MessageValue messageValue = MessageValue.builder()
						.messageIndex(i)
						.toCompleteAt(request.getToCompleteAt())
						.build();
					int topicIndex = current.nextInt(1, 3 + 1);
					kafkaTemplate.send(StreamsConfigurer.TOPIC_PREFIX + topicIndex, now, messageValue);
					log.info("sent message: " + i);
				});
			return null;
		});

		TestResponse response = TestResponse.builder().toCompleteAt(request.getToCompleteAt()).key(now).build();
		log.info("End sending {} messages for key:{} .", () -> request.getToCompleteAt(), () -> now);
		return response;
	}

	public static final String DATETIME_PATTERN_yyyyMMddHHmmss = "yyyyMMddHHmmss";
	public static final DateTimeFormatter DATETIME_FORMATTER_yyyyMMddHHmmss = DateTimeFormatter.ofPattern(DATETIME_PATTERN_yyyyMMddHHmmss);
}
