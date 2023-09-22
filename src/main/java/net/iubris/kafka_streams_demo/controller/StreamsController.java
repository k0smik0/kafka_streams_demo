package net.iubris.kafka_streams_demo.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j2;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import net.iubris.kafka_streams_demo.service.StreamsService;

/**
 * @author massimiliano.leone@capgemini.com
 *
 *         20 set 2023
 *
 */
@Log4j2
@RestController
public class StreamsController {

	@Autowired
	private StreamsService service;

	/**
	 * a simple API do send messages to input topics - in parallel
	 * @param request
	 * @return
	 */
	@ApiResponse(responseCode = HTTP_STATUS__FOUND__CODE, content = @Content(schema = @Schema(implementation = TestResponse.class)))
	@ApiResponse(responseCode = HTTP_STATUS__SERVER_ERROR__CODE, content = @Content(schema = @Schema(implementation = TestResponse.class)))
	@Operation(tags = "test")
	@PostMapping("aggregationTest")
	protected ResponseEntity<TestResponse> start(@RequestBody final TestRequest request) {
		try {
			TestResponse response = service.execute(request);
			return ResponseEntity.ok(response);
		} catch (Exception e) {
			log.error("internal error", e);
			return ResponseEntity.internalServerError().build();
		}
	}

	@AllArgsConstructor
	@NoArgsConstructor
	@Data
	public static class TestRequest {
//		private String key;
		private int toCompleteAt;
	}

	@AllArgsConstructor
	@NoArgsConstructor
	@Builder
	@Data
	public static class TestResponse {
//		private StreamTestResponseStatus status;
		private String key;
		private int toCompleteAt;
	}

	public enum StreamTestResponseStatus {
		OK_PUBLISHED,
		KO_FAILURE,
		KO_FAILURE_AFTER_TIMEOUT,
		BAD_REQUEST;
	}

	private final String HTTP_STATUS__FOUND__CODE = "200";

	private final String HTTP_STATUS__SERVER_ERROR__CODE = "500";

}
