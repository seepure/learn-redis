package org.seepure.db.redisson;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.redisson.Redisson;
import org.redisson.api.BatchResult;
import org.redisson.api.RBatch;
import org.redisson.api.RFuture;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.ClusterServersConfig;
import org.redisson.config.Config;
import org.redisson.config.MasterSlaveServersConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedissonBatchOrder {

    private static Logger LOG = LoggerFactory.getLogger(RedissonBatchOrder.class);

    public static void main(String[] args) {
        String arg = args != null && args.length >= 1 ? args[0] :
                "redis.mode=cluster;redis.nodes=redis://192.168.234.137:7000,redis://192.168.234.137:7001,redis://192.168.234.136:7000,redis://192.168.234.136:7001,redis://192.168.234.134:7000,redis://192.168.234.134:7001";
        Map<String, String> configMap = getArgMapFromArgs(arg);

        Config config = buildRedissonConfig(configMap);
        int batchSize = Integer.parseInt(configMap.getOrDefault("batchSize", "100"));
        String prefix = configMap.getOrDefault("prefix", "kk_");
        RedissonClient client = Redisson.create(config);

        //batchSet(client, prefix, batchSize);
        batchGet(client, prefix, batchSize);

        client.shutdown();
    }

    private static void batchSet(RedissonClient client, String prefix, int batchSize) {
        RBatch batch = client.createBatch();
        for (int i = 0; i < batchSize; i++) {
            batch.getBucket(prefix + i).setAsync(i, 900, TimeUnit.SECONDS);
        }
        batch.execute();
    }

    private static void batchGet(RedissonClient client, String prefix, int batchSize) {
        RBatch batch = client.createBatch();
        Random random = new Random();
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < batchSize; i++) {
            int r = random.nextInt(batchSize * 2);
            keys.add(prefix + r);
        }
        List<String> results = new ArrayList<>(keys.size());
        for (String key : keys) {
            RFuture<Object> rFuture = batch.getBucket(key).getAsync();
            rFuture.whenComplete((res, ex) -> {
                results.add(key + "=" + res);
            });
        }
        batch.execute();
        System.out.println(keys.toString());
        System.out.println(results);
    }

    private static Map<String, String> getArgMapFromArgs(String arg) {
        Map<String, String> record = new LinkedHashMap<>();
        if (StringUtils.isNotBlank(arg)) {
            LOG.debug("arg: " + arg);
            String[] kvs = StringUtils.split(arg, ';');
            LOG.debug("kvs.length = " + kvs.length);
            for (String kv : kvs) {
                String[] kvPair = StringUtils.split(kv, '=');
                record.put(kvPair[0], kvPair[1]);
            }
        }
        return record;
    }

    private static Config buildRedissonConfig(Map<String, String> configMap) {
        String mode = configMap.getOrDefault("redis.mode", "").toLowerCase();
        String nodes = configMap.getOrDefault("redis.nodes", "redis://192.168.234.137:6379");
        String password = configMap.get("redis.auth");
        Config config = new Config();
        config.setCodec(StringCodec.INSTANCE);
        switch (mode) {
            case "cluster":
                ClusterServersConfig clusterServersConfig = config.useClusterServers();
                clusterServersConfig.addNodeAddress(nodes.split(","));
                if (StringUtils.isNotBlank(password)) {
                    clusterServersConfig.setPassword(password);
                }
                break;
            default:
                MasterSlaveServersConfig masterSlaveServersConfig = config.useMasterSlaveServers();
                masterSlaveServersConfig.setMasterAddress(nodes);
                if (StringUtils.isNotBlank(password)) {
                    masterSlaveServersConfig.setPassword(password);
                }
                break;
        }
        return config;
    }
}
