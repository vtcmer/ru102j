package com.redislabs.university.RU102J.dao;

import com.redislabs.university.RU102J.api.Site;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.util.*;

public class SiteDaoRedisImpl implements SiteDao {
    private final JedisPool jedisPool;

    public SiteDaoRedisImpl(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    // When we insert a site, we set all of its values into a single hash.
    // We then store the site's id in a set for easy access.
    @Override
    public void insert(Site site) {
        try (Jedis jedis = jedisPool.getResource()) {
            String hashKey = RedisSchema.getSiteHashKey(site.getId());
            String siteIdKey = RedisSchema.getSiteIDsKey();
            jedis.hmset(hashKey, site.toMap());
            jedis.sadd(siteIdKey, hashKey);
        }
    }

    @Override
    public Site findById(long id) {
        try(Jedis jedis = jedisPool.getResource()) {
            String key = RedisSchema.getSiteHashKey(id);
            Map<String, String> fields = jedis.hgetAll(key);
            if (fields == null || fields.isEmpty()) {
                return null;
            } else {
                return new Site(fields);
            }
        }
    }

    // Challenge #1
    @Override
    public Set<Site> findAll() {
        Set<Site> result = new HashSet<>();
        // START Challenge #1
        try (Jedis jedis = jedisPool.getResource()) {
            String siteIdKey = RedisSchema.getSiteIDsKey();
            Set<String> keys = jedis.smembers(siteIdKey);
            for (String key: keys){
            //ScanResult<String> keys = jedis.sscan(siteIdKey,ScanParams.SCAN_POINTER_START);
            //for (String key: keys.getResult()){
                /*
                ScanResult<Map.Entry<String, String>> scanResult = jedis.hscan(key,ScanParams.SCAN_POINTER_START);
                Map<String,String> data = new HashMap<>();
                for (Map.Entry<String, String> entry: scanResult.getResult()){
                    data.put(entry.getKey(), entry.getValue());
                }
                 */
                Map<String,String> data =  jedis.hgetAll(key);
                Site site = new Site(data);
                result.add(site);

            }



        }

        if ((result != null) && (!result.isEmpty())) {
            return result;
        } else {
            return Collections.emptySet();
        }
        // END Challenge #1
    }
}
