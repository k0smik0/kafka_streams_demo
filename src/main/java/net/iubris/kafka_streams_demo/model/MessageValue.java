package net.iubris.kafka_streams_demo.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Builder
@Data
public class MessageValue {
	int messageIndex;
//	String word;
	int toCompleteAt;
}
