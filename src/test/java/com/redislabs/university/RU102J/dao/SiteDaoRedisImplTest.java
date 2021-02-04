package com.redislabs.university.RU102J.dao;

import com.redislabs.university.RU102J.HostPort;
import com.redislabs.university.RU102J.TestKeyManager;
import com.redislabs.university.RU102J.api.MeterReading;
import com.redislabs.university.RU102J.api.Site;
import org.junit.*;
import redis.clients.jedis.*;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;

public class SiteDaoRedisImplTest {

    private static JedisPool jedisPool;
    private static Jedis jedis;
    private static TestKeyManager keyManager;
    private Set<Site> sites;

    @BeforeClass
    public static void setUp() throws Exception {
        String password = HostPort.getRedisPassword();

        if (password.length() > 0) {
            jedisPool = new JedisPool(new JedisPoolConfig(), HostPort.getRedisHost(), HostPort.getRedisPort(), 2000, password);
        } else {
            jedisPool = new JedisPool(HostPort.getRedisHost(), HostPort.getRedisPort());
        }

        jedis = new Jedis(HostPort.getRedisHost(), HostPort.getRedisPort());

        if (password.length() > 0) {
            jedis.auth(password);
        }

        keyManager = new TestKeyManager("test");
    }

    @AfterClass
    public static void tearDown() {
        jedisPool.destroy();
        jedis.close();
    }

    @After
    public void flush() {
        keyManager.deleteKeys(jedis);
    }

    @Before
    public void generateData() {
        sites = new HashSet<>();
        sites.add(new Site(1, 4.5, 3, "123 Willow St.",
                "Oakland", "CA", "94577" ));
        sites.add(new Site(2, 3.0, 2, "456 Maple St.",
                 "Oakland", "CA", "94577" ));
        sites.add(new Site(3, 4.0, 3, "789 Oak St.",
                 "Oakland", "CA", "94577" ));
    }

    /**
     * Challenge #0 Part 1. This challenge is explained in
     * the video "How to Solve a Sample Challenge"
     */
    @Test
    public void findByIdWithExistingSite() {
        SiteDaoRedisImpl dao = new SiteDaoRedisImpl(jedisPool);
        Site site = new Site(4L, 5.5, 4, "910 Pine St.",
                "Oakland", "CA", "94577");
        dao.insert(site);
        Site storedSite = dao.findById(4L);
        assertThat(storedSite, is(site));
    }

    /**
     * Challenge #0 Part 2. This challenge is explained in
     * the video "How to Solve a Sample Challenge"
     */
    @Test
    public void findByIdWithMissingSite() {
        SiteDaoRedisImpl dao = new SiteDaoRedisImpl(jedisPool);
        assertThat(dao.findById(4L), is(nullValue()));
    }

    /**
     * Challenge #1 Part 1. Use this test case to
     * implement the challenge in Chapter 1.
     */
    //@Ignore
    @Test
    public void findAllWithMultipleSites() {
        SiteDaoRedisImpl dao = new SiteDaoRedisImpl(jedisPool);
        // Insert all sites
        for (Site site : sites) {
            dao.insert(site);
        }


        assertThat(dao.findAll(), is(sites));
    }

    /**
     * Challenge #1 Part 2. Use this test case to
     * implement the challenge in Chapter 1.
     */
    //@Ignore
    @Test
    public void findAllWithEmptySites() {
        SiteDaoRedisImpl dao = new SiteDaoRedisImpl(jedisPool);
        assertThat(dao.findAll(), is(empty()));
    }

    @Test
    public void insert() {
        SiteDaoRedisImpl dao = new SiteDaoRedisImpl(jedisPool);
        Site site = new Site(4, 5.5, 4, "910 Pine St.",
                "Oakland", "CA", "94577");
        dao.insert(site);

        Map<String, String> siteFields = jedis.hgetAll(RedisSchema.getSiteHashKey(4L));
        assertEquals(siteFields, site.toMap());

        assertThat(jedis.sismember(RedisSchema.getSiteIDsKey(), RedisSchema.getSiteHashKey(4L)),
                is(true));
    }

    @Ignore
    @Test
    public void homeWork2() {
        /*
        for (int i=0; i < 10; i++) {
            Jedis jedis = jedisPool.getResource();
            jedis.set(String.valueOf(i), "0");
            jedis.get(String.valueOf(i));
        }

         */
    }

    @Ignore
    @Test
    public void homeWork4() {
        try (Jedis jedis = jedisPool.getResource()) {
            Long setSize = jedis.scard("messages");
            System.out.println("Size: " + String.valueOf(setSize));
            Long result = jedis.srem("messages");
            jedis.close();
        }
    }



    @Test
    public void hwFinal4() {
      try (Jedis jedis = jedisPool.getResource()) {
            jedis.set("foo", "bar");
            jedis.incr("foo");
        }
    }

    @Test
    public void finalWeek512() {
        for (int i=0; i<10000; i++) {
            Jedis jedis = jedisPool.getResource();
            jedis.incr("counter");
            jedis.close();
            // CODE
        }
    }

    @Test
    public void finalWeek521() {
        insert(0, "A");
        insert(1, "B");
        insert(2, "C");
        insert(3, "A");

        try (Jedis jedis = jedisPool.getResource()) {
            Set<String> results = jedis.zrange("metrics", 0, -1);
            System.out.println(results);
        }
    }

    private void insert(Integer minuteOfDay, String element) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.zadd("metrics", minuteOfDay, element);
        }
    }


    @Test
    public void finalWeek528() {
        Map<String,String> data = new HashMap<>();
        data.put("v1","25");
        store("aaaa",data);
    }

    public Long store(String userId ,Map<String, String> action) {
        Long result = 0L;
        try (Jedis jedis = jedisPool.getResource()) {

            String userStream = "user-" + String.valueOf(userId);
            Pipeline p = jedis.pipelined();
            p.xadd("global", StreamEntryID.NEW_ENTRY, action);
            p.xadd(userStream, StreamEntryID.NEW_ENTRY, action);
            Response<Long> length = p.xlen(userStream);
            p.sync();
            result = length.get();
        }

        return result;
    }

    @Test
    public void finalWeek534() {
        updateTemperature(5.0);
    }

    public void updateTemperature(Double currentTemperature) {
        try (Jedis jedis = jedisPool.getResource()) {
            String maxTemperature = jedis.hget("metrics", "maxTemp");
            if (currentTemperature > Double.valueOf(maxTemperature)) {
                String newMax = String.valueOf(currentTemperature);
                jedis.hset("metrics", "maxTemp", newMax);
            }
        }
    }

    @Test
    public void finalWeek535() {
        try {
            hit("key",5);
        } catch (RateLimitExceededException e) {
            e.printStackTrace();
        }
    }

    public void hit(String userId, Integer maxHits) throws RateLimitExceededException {
        try (Jedis jedis = jedisPool.getResource()) {
            String key = "limiter-" + Instant.now().getEpochSecond() +
                    "-" + userId;
            Pipeline p = jedis.pipelined();
            p.lpush(key, Instant.now().toString());
            p.expire(key, 1);
            Response<List<String>> responses = p.lrange(key, 0, -1);
            p.sync();
            if (responses.get().size() > maxHits) {
                throw new RateLimitExceededException();
            }
        }
    }

}