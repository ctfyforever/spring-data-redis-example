package com.devpg.redis;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.GenericToStringSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.data.redis.support.atomic.RedisAtomicInteger;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Spring Data - Redis provides wrapper classes for all Redis data types
 * (String, List, Set, ...) which encapsulate the supported operations by the
 * store. This test case shows to usage of RedisAtomicInteger and the pitfall
 * when mixing the usage of wrapper classes and RedisTemplate.
 * 
 * @author aneubauer
 * 
 */

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "/testContext.xml" })
public class RedisIntegerDataTypTest {

	Logger logger = LoggerFactory.getLogger(RedisIntegerDataTypTest.class);

	@Autowired
	private RedisTemplate<String, Integer> template;

	private final String counterKey = "type-counter";
	RedisAtomicInteger counterWrapper;

	@Before
	public void setupDataTypes() {
		/*
		 * Wrap key "type-counter" as RedisAtomicInteger and set value to 1 (by
		 * default this value will be set to 0 if the key doesn't exist)
		 */
		counterWrapper = new RedisAtomicInteger(counterKey,
				template.getConnectionFactory(), 1);
	}

	@After
	public void deleteExampleDataTypes() {
		template.delete(counterKey);
	}

	@Test
	public void accessCounterAUsingWrapperClass() {
		int count = counterWrapper.getAndIncrement();
		Assert.assertEquals(count + 1, counterWrapper.get());

		count = counterWrapper.getAndDecrement();
		Assert.assertEquals(count - 1, counterWrapper.get());
	}

	@Test
	public void accessCounterUsingRedisTemplate() {

		/*
		 * Instead of using the wrapper classes you can also utilize
		 * RedisTemplate.
		 * 
		 * Attention: Spring Data - Redis uses serializers to serialize and
		 * deserialize Objects to byte array (binary data). Due to different
		 * key/value serializer (RedisAtomicInteger is using
		 * StringRedisSerializer while RedisTemplate is using
		 * JdkSerializationRedisSerializer by default) the same key cannot be
		 * accessed by the other. To make it work you have to equalize the
		 * serializers.
		 */

		template.setKeySerializer(new StringRedisSerializer());
		template.setValueSerializer(new GenericToStringSerializer<Integer>(
				Integer.class));
		Assert.assertNotNull(template.boundValueOps(counterKey).get());
	}
}
