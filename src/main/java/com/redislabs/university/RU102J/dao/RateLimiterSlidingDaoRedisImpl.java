package com.redislabs.university.RU102J.dao;

import com.redislabs.university.RU102J.core.KeyHelper;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

import java.time.ZonedDateTime;
import java.util.UUID;

public class RateLimiterSlidingDaoRedisImpl implements RateLimiter {

    private final JedisPool jedisPool;
    private final long windowSizeMS;
    private final long maxHits;

    public RateLimiterSlidingDaoRedisImpl(JedisPool pool, long windowSizeMS,
                                          long maxHits) {
        this.jedisPool = pool;
        this.windowSizeMS = windowSizeMS;
        this.maxHits = maxHits;
    }

    // Challenge #7
    @Override
    public void hit(String name) throws RateLimitExceededException {
        // START CHALLENGE #7
        try(Jedis jedis = jedisPool.getResource()){
            String key = KeyHelper.getKey("limiter:"+this.windowSizeMS+":"+name+":"+this.maxHits);
            long now = ZonedDateTime.now().toInstant().toEpochMilli();

            Transaction tx = jedis.multi();
            String member = now+"-"+Math.random();
            tx.zadd(key,now, member); // Añadir
            tx.zremrangeByScore(key,0, now - this.windowSizeMS);
            Response<Long> response =  tx.zcard(key);
            tx.exec();

            if (response.get() > this.maxHits){
                throw new RateLimitExceededException();
            }

        }
        // END CHALLENGE #7
    }
}
