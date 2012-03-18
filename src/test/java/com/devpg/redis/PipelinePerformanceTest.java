package com.devpg.redis;

import java.util.Random;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "/testContext.xml" })
public class PipelinePerformanceTest {

	Logger logger = LoggerFactory.getLogger(PipelinePerformanceTest.class);

	@Autowired
	RedisTemplate<String, Integer> template;

	private final int elementsCount = 100000;
	private final String seqFilledSet = "perf-set-seq";
	private String pipelinedSet = "perf-set-pipe";

	@After
	public void dropData() {
		template.delete(seqFilledSet);
		template.delete(pipelinedSet);
	}

	@Test
	public void measureSequentialSAdd() {
		Random randomGenerator = new Random();

		long start = System.currentTimeMillis();
		for (int i = 1; i <= elementsCount; i++) {
			template.opsForSet().add(seqFilledSet, randomGenerator.nextInt());
		}
		long duration = System.currentTimeMillis() - start;
		logger.info(seqFilledSet + ": " + (elementsCount / duration)
				+ " SADD/sec");
	}

	@Test
	public void measurePipelinedSAdd() {
		final int chunks = 10;
		final int chunkSize = elementsCount / chunks;
		final Random randomGenerator = new Random();

		final RedisSerializer keySerializer = template.getKeySerializer();
		final RedisSerializer valueSerializer = template.getValueSerializer();

		final RedisCallback<Void> pipedSAddCallback = new RedisCallback<Void>() {

			public Void doInRedis(RedisConnection connection) {
				for (int j = 0; j < chunkSize; j++) {
					byte[] key = keySerializer.serialize(pipelinedSet);
					byte[] value = valueSerializer.serialize(randomGenerator
							.nextInt());
					connection.sAdd(key, value);
				}
				return null;
			}
		};

		long start = System.currentTimeMillis();
		for (int i = 0; i <= chunks; i++) {
			template.execute(pipedSAddCallback, true, true);
		}
		long duration = System.currentTimeMillis() - start;
		logger.info(pipelinedSet + " " + (elementsCount / duration)
				+ " SADD/sec");
	}
}
