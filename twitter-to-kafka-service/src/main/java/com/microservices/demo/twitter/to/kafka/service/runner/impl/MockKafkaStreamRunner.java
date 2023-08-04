package com.microservices.demo.twitter.to.kafka.service.runner.impl;
import com.microservices.demo.twitter.to.kafka.service.config.TwitterToKafkaServiceConfigData;
import com.microservices.demo.twitter.to.kafka.service.exception.TwitterToKafkaServiceException;
import com.microservices.demo.twitter.to.kafka.service.listener.TwitterKafkaStatusListener;
import com.microservices.demo.twitter.to.kafka.service.runner.StreamRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
@Component
@ConditionalOnProperty(name = "twitter-to-kafka-service.enable-mock-tweets", havingValue = "true")
public class MockKafkaStreamRunner implements StreamRunner {

	private static final Logger LOG = LoggerFactory.getLogger(MockKafkaStreamRunner.class);

	private final TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData;
	private final TwitterKafkaStatusListener twitterKafkaStatusListener;

	private static final Random RANDOM = new Random();

	private static final String[] WORDS = new String[]{
			"Lorem",
			"ipsum",
			"dolor",
			"sit",
			"amet",
			"consectetuer",
			"adipiscing",
			"elit",
			"Maecenas",
			"porttitor",
			"congue",
			"massa",
			"Fusce",
			"posuere",
			"magna",
			"sed",
			"pulvinar",
			"ultricies",
			"purus",
			"lectus",
			"malesuada",
			"libero"
	};

	private static final String tweetAsRawJson = "{" +
			"\"created_at\":\"{0}\"," +
			"\"id\":\"{1}\"," +
			"\"text\":\"{2}\"," +
			"\"user\":{\"id\":\"{3}\"}" +
			"}";

	private static final String TWITTER_STATUS_DATE_FORMAT = "EEE MMM dd HH:mm:ss zzz yyyy";


	public MockKafkaStreamRunner(TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData, TwitterKafkaStatusListener twitterKafkaStatusListener) {
		this.twitterToKafkaServiceConfigData = twitterToKafkaServiceConfigData;
		this.twitterKafkaStatusListener = twitterKafkaStatusListener;
	}
	@Override
	public void start() throws TwitterException {
		String[] keywords = twitterToKafkaServiceConfigData.getTwitterKeywords().toArray(new String[0]);
		int minTweetLength = twitterToKafkaServiceConfigData.getMockMinTweetLength();
		int maxTweetLength = twitterToKafkaServiceConfigData.getMockMaxTweetLength();
		long sleepTimeMs = twitterToKafkaServiceConfigData.getMockSleepMs();
		LOG.info("Starting mock filtering twitter streams for keywords {}", Arrays.toString(keywords));
		simulateTwitterStream(keywords, minTweetLength, maxTweetLength, sleepTimeMs);
	}
	private void simulateTwitterStream(final String[] keywords, final int minTweetLength, final int maxTweetLength, final long sleepTimeMs) {
		Executors.newSingleThreadExecutor().submit(() -> {
			try {
				while (true) {
					String formattedTweetAsRawJson = getFormmattedTweet(keywords, minTweetLength, maxTweetLength);
					Status status = TwitterObjectFactory.createStatus(formattedTweetAsRawJson);
					twitterKafkaStatusListener.onStatus(status);
					sleep(sleepTimeMs);
				}
			} catch (TwitterException e) {
				LOG.error("Error creating Twitter status", e);
			}
		});
	}
	private void sleep(final long sleepTimeMs) {
		try {
			Thread.sleep(sleepTimeMs);
		} catch (InterruptedException e) {
			throw new TwitterToKafkaServiceException("Error while sleeping for waiting for new status to be created");
		}
	}
	private String getFormmattedTweet(final String[] keywords, final int minTweetLength, final int maxTweetLength) {
		String[] params = new String[] {
				ZonedDateTime.now().format(DateTimeFormatter.ofPattern(TWITTER_STATUS_DATE_FORMAT, Locale.ENGLISH)),
				String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE)),
				getRandomTweetContent(keywords, minTweetLength, maxTweetLength),
				String.valueOf(ThreadLocalRandom.current().nextLong(Long.MAX_VALUE))
		};
		return formatTweetAsJson(params);
	}
	private static String formatTweetAsJson(final String[] params) {
		String tweet = tweetAsRawJson;
		for (int i = 0; i < params.length; i++) {
			tweet = tweet.replace("{" + i + "}", params[i]);
		}
		return tweet;
	}
	private String getRandomTweetContent(final String[] keywords, final int minTweetLength, final int maxTweetLength) {
		StringBuilder tweet = new StringBuilder();
		int tweetLength = RANDOM.nextInt(maxTweetLength - minTweetLength + 1) + minTweetLength;
		return constructRandomTweet(keywords, tweetLength, tweet);
	}
	private static String constructRandomTweet(final String[] keywords, final int tweetLength, final StringBuilder tweet) {
		for (int i = 0; i < tweetLength; i++) {
			tweet.append(WORDS[RANDOM.nextInt(WORDS.length)]).append(" ");
			if (i == tweetLength / 2) {
				tweet.append(keywords[RANDOM.nextInt(keywords.length)]).append(" ");
			}
		}
		return tweet.toString().trim();
	}
}
