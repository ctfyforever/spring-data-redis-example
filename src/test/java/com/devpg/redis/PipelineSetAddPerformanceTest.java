package com.devpg.redis;

import java.util.Random;

import org.junit.After;
import org.junit.Assert;
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
public class PipelineSetAddPerformanceTest {

	Logger logger = LoggerFactory
			.getLogger(PipelineSetAddPerformanceTest.class);

	@Autowired
	RedisTemplate<String, Integer> template;

	private final int elementsCount = 100000;
	private final String sequentialFilledSet = "perf-set-seq";
	private String pipelinedSet = "perf-set-pipe";

	@After
	public void deleteSampleData() {
		template.delete(sequentialFilledSet);
		template.delete(pipelinedSet);
	}

	@Test
	public void pipelineAddShouldBeFasterThanSequentialMode() {
		final Double durationSequentialAdd = getDurationForSequentialAdd();
		logger.info("Sequential SAdd took {} ms ({} SADD/sec)", new Object[] {
				durationSequentialAdd, durationSequentialAdd / elementsCount });

		final Integer[] chunkDefinitions = new Integer[] { 10000, 1000, 100, 10 };
		for (Integer chunkDefinition : chunkDefinitions) {
			final Double durationPipelinedAdd = getDurationForPipelinedAdd(chunkDefinition);
			logger.info(
					"Pipelined SAdd (chunks: {}) took {} ms ({} SADD/sec)",
					new Object[] { chunkDefinition, durationPipelinedAdd,
							durationPipelinedAdd / elementsCount });

			Assert.assertTrue(durationPipelinedAdd < durationSequentialAdd);
		}
	}

	private double getDurationForSequentialAdd() {
		final long start = System.currentTimeMillis();
		final Random randomGenerator = new Random();

		for (int i = 1; i <= elementsCount; i++) {
			template.opsForSet().add(sequentialFilledSet,
					randomGenerator.nextInt());
		}
		return System.currentTimeMillis() - start;
	}

	private double getDurationForPipelinedAdd(int chunks) {
		final long start = System.currentTimeMillis();

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

		for (int i = 0; i <= chunks; i++) {
			template.execute(pipedSAddCallback, true, true);
		}
		return System.currentTimeMillis() - start;
	}
}
