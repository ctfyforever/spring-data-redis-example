package com.devpg.redis;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({ AtomicCounterTest.class, PipelineSetAddPerformanceTest.class,
		SetIsMemberPerformanceTest.class, TransactionTest.class })
public class SpringDataRedisSampleSuite {

}
