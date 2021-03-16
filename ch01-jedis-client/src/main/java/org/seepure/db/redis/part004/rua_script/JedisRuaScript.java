package org.seepure.db.redis.part004.rua_script;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;

public class JedisRuaScript {

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

            String key = "hello";
            String script =  "return redis.call('get',KEYS[1])";
            Object result = jedis.eval(script, Arrays.asList(key), new ArrayList<>());
            System.out.println(result);

            script = loadScript();
            result = jedis.eval(script, Arrays.asList("hot:user:list"), new ArrayList<>());
            System.out.println(result);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (jedis != null)
                jedis.close();
        }
    }

    private static String loadScript() throws IOException {
        InputStream inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("lrange_and_mincr.lua");
        InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
        char[] chars = new char[1024];
        inputStreamReader.read(chars);
        String result = new String(chars);
        return result;
    }
}
