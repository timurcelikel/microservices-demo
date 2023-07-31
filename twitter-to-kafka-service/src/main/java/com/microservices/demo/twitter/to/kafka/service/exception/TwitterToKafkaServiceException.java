package com.microservices.demo.twitter.to.kafka.service.exception;
public class TwitterToKafkaServiceException extends RuntimeException {

	public TwitterToKafkaServiceException() {
		super();
	}
	public TwitterToKafkaServiceException(final String message) {
		super(message);
	}
	public TwitterToKafkaServiceException(final String message, final Throwable cause) {
		super(message, cause);
	}
}
