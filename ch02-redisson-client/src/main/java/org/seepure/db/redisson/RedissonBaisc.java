package org.seepure.db.redisson;

import org.redisson.Redisson;
import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

public class RedissonBaisc {
    public static void main(String[] args) {
        if (args == null || args.length < 2) {
            System.err.println("usage : get/set key value");
            //System.exit(1);
            args = new String[] {"get", "key1"};
            //args = new String[] {"set", "key1", "shengshashou"};
        }
        Config config = new Config();
        //config.setTransportMode(TransportMode.NIO);
        //config.setCodec(StringCodec.INSTANCE);
//        config.useClusterServers()
//                .setPassword("5+0u%acwzSzzRTmsF")
//                .addNodeAddress("redis://xxx:9006", "redis://xxx:9006");

        config.useSingleServer()
                .setPassword("222:datahubtest")
                .setAddress("redis://xxx:12002");

        //config.useSentinelServers().addSentinelAddress("redis://xxx:12002", "redis://xxx:12002");

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
