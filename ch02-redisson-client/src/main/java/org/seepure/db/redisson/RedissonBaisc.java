package org.seepure.db.redisson;

import org.redisson.Redisson;
import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;
import org.redisson.config.TransportMode;

public class RedissonBaisc {
    public static void main(String[] args) {
        if (args == null || args.length < 2) {
            System.err.println("usage : get/set key value");
            System.exit(1);
        }
        Config config = new Config();
        config.setTransportMode(TransportMode.NIO);
        config.setCodec(StringCodec.INSTANCE);
        config.useClusterServers()
                .setPassword("222:datahubtest")
                .setScanInterval(2000)
                .addNodeAddress("redis://100.66.1.156:12002", "redis://10.121.98.2:12002", "redis://10.121.98.72:12002");

        RedissonClient redisson = Redisson.create(config);
        String mode = args[0];
        if (mode.equalsIgnoreCase("get")) {
            doGetMode(redisson, args);
        } else if (mode.equalsIgnoreCase("set")) {
            doSetMode(redisson, args);
        }

    }

    private static void doSetMode(RedissonClient redisson, String[] args) {
        String key = args[1];
        String value = args[2];
        RBucket<Object> bucket = redisson.getBucket(key);
        bucket.set(value);
    }

    private static void doGetMode(RedissonClient redisson, String[] args) {
        String key = args[1];
        RBucket<Object> bucket = redisson.getBucket(key);
        Object value = bucket.get();
        System.out.printf(String.valueOf(value));
    }


}
