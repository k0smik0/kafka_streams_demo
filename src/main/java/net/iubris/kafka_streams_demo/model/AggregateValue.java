package net.iubris.kafka_streams_demo.model;

import java.util.LinkedHashMap;

import lombok.Data;

@Data
public class AggregateValue {
	private int completedAt;
	private final LinkedHashMap<Integer, String> payload = new LinkedHashMap<>();
}
