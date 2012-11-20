package com.devpg.redis;

import java.util.Iterator;
import java.util.Random;

import org.junit.After;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.support.collections.DefaultRedisSet;
import org.springframework.data.redis.support.collections.RedisSet;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.CollectionUtils;

/**
 * This test case shows the usage (as well as advantage) of Redis supported data
 * operations using the example of SISMEMBER
 * 
 * @author aneubauer
 * 
 */

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "/testContext.xml" })
public class SetIsMemberPerformanceTest {

	Logger logger = LoggerFactory.getLogger(SetIsMemberPerformanceTest.class);

	@Autowired
	private RedisTemplate<String, Integer> template;
	private RedisSet<Integer> redisSet;
	private final String key = "ops-set";
	private final int elementsCount = 10;
	private final int passesForRobustAverageTime = 5;

	@Before
	public void setupRedisSetWithRandomInteger() {
		Random randomGenerator = new Random(System.currentTimeMillis());
		redisSet = new DefaultRedisSet<Integer>(key, template);
		for (int i = 0; i < elementsCount; i++) {
			redisSet.add(randomGenerator.nextInt());
		}
	}

	@After
	public void deleteExampleSet() {
		template.delete(key);
	}

	@Test
	public void redisIsMemberCommandShouldBeFasterThanLocalLookUp() {
		double averageTimeForServerSideLookUp = getAverageTimeForRedisIsMemberCommand();
		double averageTimeForLocalLookup = getAverageTimeForLocalLookUp();

		logger.info("Average server side look up (SISMEMBER) took {} ms", averageTimeForServerSideLookUp);
		logger.info("Average local look up took {} ms", averageTimeForLocalLookup);

		assertTrue(averageTimeForServerSideLookUp < averageTimeForLocalLookup);
	}

	private double getAverageTimeForRedisIsMemberCommand() {
		long start = System.currentTimeMillis();
		Random randomGenerator = new Random(System.currentTimeMillis());

		for (int i = 0; i < passesForRobustAverageTime; i++) {
			redisSet.contains(randomGenerator.nextInt());
		}
		return (double) (System.currentTimeMillis() - start) / passesForRobustAverageTime;
	}

	private double getAverageTimeForLocalLookUp() {
		long start = System.currentTimeMillis();
		Random randomGenerator = new Random(System.currentTimeMillis());

		for (int i = 0; i < passesForRobustAverageTime; i++) {
			// Create iterator with local data each time!
			Iterator<Integer> localIterator = redisSet.iterator();
			CollectionUtils.contains(localIterator, randomGenerator.nextInt());
		}
		return (double) (System.currentTimeMillis() - start) / passesForRobustAverageTime;
	}
}