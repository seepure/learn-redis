package org.seepure.db.redis.part003.pipeline;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;

import java.util.List;

public class JedisPipelineUsage {

    public static void main(String[] args) {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(100);
        config.setMaxIdle(10);
        config.setTestOnBorrow(true);
        config.setMaxWaitMillis(2000);

        JedisPool jedisPool = new JedisPool(config, "192.168.187.128", 6379, 1000);

        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
            mdel(jedis, "k1", "k2", "k4");
            pipelineReturnAll(jedis);
        } catch (Exception e) {

        } finally {
            if (jedis != null)
                jedis.close();
        }
    }

    public static void mdel(Jedis jedis, String ... keys) {
        if (jedis == null)
            return;
        Pipeline pipeline = jedis.pipelined();
        for (String key : keys) {
            pipeline.del(key);
        }
        pipeline.sync();
    }

    public static void pipelineReturnAll(Jedis jedis) {
        if (jedis == null)
            return;
        Pipeline pipeline = jedis.pipelined();
        pipeline.set("hello", "world");
        pipeline.incr("counter");
        List<Object> resultList = pipeline.syncAndReturnAll();
        for (Object object : resultList) {
            System.out.println(object);
        }
    }
}
