package com.devpg.redis;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "/testContext.xml" })
public class TransactionTest {

	Logger logger = LoggerFactory.getLogger(TransactionTest.class);

	@Autowired
	RedisTemplate<String, Integer> template;

	private final String key = "tx-key";

	@After
	public void deleteCounter() {
		template.delete(key);
	}
	
	@Test
	public void useOptimisticLocking() {
		final int valueSetInBetween = 23;
		final int valueSetWithinSession = 42;

		/*
		 * By default each template method call creates a new connection - so
		 * WATCH, MUTLI, EXEC, UNWATCH won't work because of the missing
		 * context. To make use of transaction support use SessionCallback which
		 * reuses the underlying connection.
		 */
		template.execute(new SessionCallback<Void>() {

			@Override
			public Void execute(RedisOperations operations)
					throws DataAccessException {
				operations.watch(key);

				setKeyByOtherBySession(valueSetInBetween);

				operations.multi();
				operations.boundValueOps(key).set(valueSetWithinSession);
				operations.exec();
				operations.unwatch();

				return null;
			}
		});

		int value = template.boundValueOps(key).get().intValue();
		Assert.assertEquals(valueSetInBetween, value);
	}

	private final void setKeyByOtherBySession(int value) {
		template.boundValueOps(key).set(value);
	}
}
