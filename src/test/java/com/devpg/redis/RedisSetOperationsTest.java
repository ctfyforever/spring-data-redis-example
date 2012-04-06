package com.devpg.redis;

import java.util.Iterator;

import org.junit.After;
import org.junit.Assert;
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
public class RedisSetOperationsTest {

	Logger logger = LoggerFactory.getLogger(RedisSetOperationsTest.class);

	@Autowired
	private RedisTemplate<String, Integer> template;
	private RedisSet<Integer> set;
	private final String key = "ops-set";

	@Before
	public void setupExampleData() {
		set = new DefaultRedisSet<Integer>(key, template);
		for (int i = 0; i < 1000; i++) {
			set.add(i);
		}
	}

	@After
	public void deleteExampleSet() {
		template.delete(key);
	}

	@Test
	public void checkForMembersUsingRedisOperations() {
		long start = System.currentTimeMillis();
		Assert.assertTrue(set.contains(1));
		Assert.assertFalse(set.contains(1000));
		logger.info("SISMEMBER took " + (System.currentTimeMillis() - start)
				+ " ms");
	}

	@Test
	public void checkForMembersLocally() {
		long start = System.currentTimeMillis();
		Iterator<Integer> iterator = set.iterator();
		Assert.assertTrue(CollectionUtils.contains(iterator, 1));
		Assert.assertFalse(CollectionUtils.contains(iterator, 1000));
		logger.info("Local check for members took "
				+ (System.currentTimeMillis() - start) + " ms");
	}
}
