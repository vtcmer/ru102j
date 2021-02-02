package com.redislabs.university.RU102J.dao;

import com.redislabs.university.RU102J.JedisDaoTestBase;
import com.redislabs.university.RU102J.api.Measurement;
import com.redislabs.university.RU102J.api.MeterReading;
import com.redislabs.university.RU102J.api.MetricUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

public class MetricDaoRedisZsetImplTest extends JedisDaoTestBase {

    private List<MeterReading> readings;
    private Long siteId = 1L;
    private ZonedDateTime startingDate = ZonedDateTime.now(ZoneOffset.UTC);

    @After
    public void flush() {
        keyManager.deleteKeys(jedis);
    }

    /**
     * Generate 72 hours worth of data.
     */
    @Before
    public void generateData() {
        readings = new ArrayList<>();
        ZonedDateTime time = startingDate;
        for (int i=0; i <  72 * 60; i++) {
            MeterReading reading = new MeterReading();
            reading.setSiteId(siteId);
            reading.setTempC(i * 1.0);
            reading.setWhUsed(i * 1.0);
            reading.setWhGenerated(i * 1.0);
            reading.setDateTime(time);
            readings.add(reading);
            time = time.minusMinutes(1);
        }
    }

    // Challenge #2
    //@Ignore
    @Test
    public void testSmall() {
        testInsertAndRetrieve(1);
    }

    // Challenge #2
    //@Ignore
    @Test
    public void testOneDay() {
        testInsertAndRetrieve(60 * 24);
    }


    // Challenge #2
    //@Ignore
    @Test
    public void testMultipleDays() {
        testInsertAndRetrieve(60 * 70);
    }

    private void testInsertAndRetrieve(int limit) {
        MetricDao metricDao = new MetricDaoRedisZsetImpl(jedisPool);
        for (MeterReading reading : readings) {
            metricDao.insert(reading);
        }

        List<Measurement> measurements = metricDao.getRecent(siteId, MetricUnit.WHGenerated,
         startingDate, limit);
        assertThat(measurements.size(), is(limit));
        int i = limit;
        for (Measurement measurement : measurements) {
            assertThat(measurement.getValue(), is((i - 1) * 1.0));
            i -= 1;
        }
    }


    @Test
    public void homework2_3() {
        try (Jedis jedis = jedisPool.getResource()) {

            jedis.set("a", "foo");
            jedis.set("b", "bar");
            jedis.set("c", "baz");
            Transaction t = jedis.multi();

            Response<String> r1 = t.set("b", "1");
            Response<Long> r2 = t.incr("a");
            Response<String> r3 = t.set("c", "100");

            t.exec();
            r1.get();
            r2.get();
            r3.get();

        }
    }


    @Test
    public void homework2_4() {
        try (Jedis jedis = jedisPool.getResource()) {
            Pipeline p = jedis.pipelined();
            Response<Long> length = p.zcard("set");
            if (length.get() < 1000) {
                String element = "foo" + String.valueOf(Math.random());
                p.zadd("set", Math.random(), element);
            }

            p.sync();

        }
    }

    @Test
    public void homework2_2() {
        this.getCounts(10);
    }

    private List<Long> getCounts(Integer num) {
        List<Long> results = new ArrayList<>(num);

        try (Jedis jedis = jedisPool.getResource()) {
            for (int i = 0; i < num; i++) {
                System.out.println("Contador::"+i);
                String key = String.valueOf(i);
                if (jedis.exists(key)) {
                    Long c = jedis.zcount(key, "-inf", "+inf");
                    results.add(c);
                    jedis.expire(key, 1000);
                }
            }
        }

        return results;
    }
}