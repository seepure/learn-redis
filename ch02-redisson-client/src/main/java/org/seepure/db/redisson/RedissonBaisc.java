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
            //System.exit(1);
            args = new String[]{"get", "k1"};
            args = new String[] {"set", "k2", "shengshashou"};
        }
        Config config = new Config();
        config.setTransportMode(TransportMode.NIO);
        config.setCodec(StringCodec.INSTANCE);

        //mode-1. single-node-mode
        //config.useSingleServer().setPassword("222:datahubtest").setAddress("redis://192.168.213.128:6379");

        //mode-2. master-slave-mode
        config.useMasterSlaveServers().setMasterAddress("redis://192.168.213.128:6379").addSlaveAddress("redis://192.168.213.129:6379");

        //mode-3. sentinel-mode
        //config.useSentinelServers().addSentinelAddress("redis://xxx:12002", "redis://xxx:12002");

        //mode-4. cluster mode
//        config.useClusterServers()
//                .setPassword("5+0u%acwzSzzRTmsF")
//                .addNodeAddress("redis://xxx:9006", "redis://xxx:9006");

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
