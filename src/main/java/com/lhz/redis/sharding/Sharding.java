package com.lhz.redis.sharding;

import java.util.regex.Pattern;
import java.util.zip.CRC32;

import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

@Component
public class Sharding {

	public String shardKey(String base, String key, Integer totalElement, Integer shardSize) {
		Integer shardId = 0;
		if (isInteger(key)) {
			shardId = Integer.parseInt(key) / shardSize;
		} else {
			Integer shards = 2 * totalElement / shardSize;
			CRC32 c = new CRC32();
			c.update(key.getBytes());
			shardId = (int) (c.getValue() % shards);
		}
		return String.format("%s:%s", base, shardId);
	}

	public void shardHSet(RedisTemplate<String, String> redisTemplate, String base, String key, String value,
			Integer totalElement, Integer shardSize) {
		String shard = shardKey(base, key, totalElement, shardSize);
		redisTemplate.opsForHash().put(shard, key, value);
	}
	
	public void shardRightPush(RedisTemplate<String, String> redisTemplate, String base, String key, String value,
			Integer totalElement, Integer shardSize) {
		String shard = shardKey(base, key, totalElement, shardSize);
		redisTemplate.opsForList().rightPush(shard+":"+key, value);
	}
	
	public void shardRightPop(RedisTemplate<String, String> redisTemplate, String base, String key, String value,
			Integer totalElement, Integer shardSize) {
		String shard = shardKey(base, key, totalElement, shardSize);
		redisTemplate.opsForList().leftPush(shard+":"+key, value);
	}
	
	public Object shardLeftPop(RedisTemplate<String, String> redisTemplate, String base, String key,
			Integer totalElement, Integer shardSize) {
		String shard = shardKey(base, key, totalElement, shardSize);
		return redisTemplate.opsForList().leftPop(shard+":"+key);
	}
	
	public Object shardRightPop(RedisTemplate<String, String> redisTemplate, String base, String key,
			Integer totalElement, Integer shardSize) {
		String shard = shardKey(base, key, totalElement, shardSize);
		return redisTemplate.opsForList().rightPop(shard+":"+key);
	}

	public Object shardHGet(RedisTemplate<String, String> redisTemplate, String base, String key, Integer totalElement,
			Integer shardSize) {
		String shard = shardKey(base, key, totalElement, shardSize);
		return redisTemplate.opsForHash().get(shard, key);
	}

	public static boolean isInteger(String str) {
		Pattern pattern = Pattern.compile("^[-\\+]?[\\d]*$");
		return pattern.matcher(str).matches();
	}

}
