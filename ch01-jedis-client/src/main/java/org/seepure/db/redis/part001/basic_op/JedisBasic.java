package org.seepure.db.redis.part001.basic_op;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class JedisBasic {

    public static void main(String[] args) {
        Jedis jedis = null;
        try {
            jedis = new Jedis("192.168.187.128", 6379);
            String v1 = jedis.get("hello");
            System.out.println(v1);

            //1. string: get set incr
            jedis.set("hello", "world1");
            System.out.println(jedis.get("hello"));
            System.out.println(jedis.get("counter"));
            Long counter = jedis.incr("counter");
            System.out.println(counter);

            //2. hash
            jedis.hset("myhash", "f1", "v1");
            jedis.hset("myhash", "f2", "v2");

            Map<String, String> myHash = jedis.hgetAll("myhash");
            System.out.println(myHash);

            //3. list
            jedis.rpush("mylist", "1");
            jedis.rpush("mylist", "2");
            jedis.rpush("mylist", "3");
            List<String> myList = jedis.lrange("mylist", 0, -1);
            System.out.println(myList);

            //4. set
            System.out.println(jedis.sadd("myset", "a"));
            jedis.sadd("myset", "b");
            System.out.println(jedis.sadd("myset", "a"));
            Set<String> mySet = jedis.smembers("myset");
            System.out.println(mySet);

            //5. zset
            jedis.zadd("myzset", 99, "tom");
            jedis.zadd("myzset", 66, "peter");
            jedis.zadd("myzset", 33, "james");
            Set<Tuple> myZset = jedis.zrangeWithScores("myzset", 0, -1);
            System.out.println(myZset);

            //6. bitmap
            jedis.setbit("u:user:20200129", 1, true);
            jedis.setbit("u:user:20200129", 4, true);
            jedis.setbit("u:user:20200129", 9, true);
            System.out.println(jedis.bitcount("u:user:20200129"));
            System.out.println(jedis.getbit("u:user:20200129", 0));
            System.out.println(jedis.getbit("u:user:20200129", 1));

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (jedis != null)
                jedis.close();
        }
    }
}
