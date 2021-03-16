package org.seepure.db.redis.part002.jedis_pool;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class JedisPoolUsage {

    public static void main(String[] args) {
//        GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
//        poolConfig.setTestOnBorrow(true);
        //1. init the JedisPool
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(100);
        config.setMaxIdle(10);
        config.setTestOnBorrow(true);
        config.setMaxWaitMillis(2000);

        JedisPool jedisPool = new JedisPool(config, "192.168.187.128", 6379, 1000);

        Jedis jedis = null;
        try {
            //2. get connection from the jedisPool
            jedis = jedisPool.getResource();

            String v1 = jedis.get("hello");
            System.out.println(v1);
            jedis.set("hello", "world1");
            System.out.println(jedis.get("hello"));
            System.out.println(jedis.get("counter"));
            Long counter = jedis.incr("counter");
            System.out.println(counter);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            //3.watch out! Don't use jedisPool.returnResource(), use jedis.close() instead.
            if (jedis != null)
                jedis.close();

            //jedis.close source code
//            public void close() {
//                if (dataSource != null) {
//                    JedisPoolAbstract pool = this.dataSource;
//                    this.dataSource = null;
//                    if (client.isBroken()) {
//                        pool.returnBrokenResource(this);
//                    } else {
//                        pool.returnResource(this);
//                    }
//                } else {
//                    super.close();
//                }
//            }
        }
    }
}
