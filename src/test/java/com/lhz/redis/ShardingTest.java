package com.lhz.redis;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;

import com.lhz.redis.sharding.Sharding;

@RunWith(SpringRunner.class)
@SpringBootTest
@WebAppConfiguration  
public class ShardingTest {
	
	private static final Logger log = LoggerFactory.getLogger(ShardingTest.class);
	
	@Autowired
	private Sharding sharding;
	@Autowired
	private RedisTemplate<String, String> redisTemplate;
	
	public static final Integer totalElement = 32000;
	public static final Integer shardSize = 1024;
	
	@Test
	public void contextLoads() {
		sharding.shardHSet(redisTemplate, "author", "kjhkjabsjdhgaskd111", "22", totalElement, shardSize);
	}
	
	@Test
	public void get() {
		Object s = sharding.shardHGet(redisTemplate, "author", "kjhkjabsjdhgaskd111", totalElement, shardSize);
		log.info(s.toString());
	}
	
	@Test
	public void rPush() {
		sharding.shardRightPush(redisTemplate, "notice", "kjhkjabsjdhgaskd111","asdasd", totalElement, shardSize);
	}
	
	@Test
	public void rPop() {
		Object s = sharding.shardLeftPop(redisTemplate, "notice", "kjhkjabsjdhgaskd111", totalElement, shardSize);
		log.info(s.toString());
	}

}
