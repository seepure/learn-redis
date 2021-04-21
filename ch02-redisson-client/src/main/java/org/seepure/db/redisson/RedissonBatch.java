package org.seepure.db.redisson;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.redisson.Redisson;
import org.redisson.api.BatchResult;
import org.redisson.api.RBatch;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.seepure.db.redisson.config.ConfigUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedissonBatch {

    private static Logger LOG = LoggerFactory.getLogger(RedissonBatch.class);

    public static void main(String[] args) {
        String arg = args != null && args.length >= 1 ? args[0]
                : "mode=cluster;nodes=redis://192.168.234.137:7000,redis://192.168.234.137:7001,redis://192.168.234.138:7000,redis://192.168.234.138:7001,redis://192.168.234.134:7000,redis://192.168.234.134:7001";
        Map<String, String> configMap = ConfigUtil.getArgMapFromArgs(arg);

        Config config = ConfigUtil.buildRedissonConfig(configMap);
        int batchSize = Integer.parseInt(configMap.getOrDefault("batchSize", "100"));
        String prefix = configMap.getOrDefault("prefix", "kk_");
        RedissonClient client = Redisson.create(config);

        batchSet(client, prefix, batchSize);
        batchGet(client, prefix, batchSize);

        client.shutdown();
    }

    private static void batchSet(RedissonClient client, String prefix, int batchSize) {
        RBatch batch = client.createBatch();
        for (int i = 0; i < batchSize; i++) {
            batch.getBucket(prefix + i).setAsync(i, 100, TimeUnit.SECONDS);
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
        for (String key : keys) {
            batch.getBucket(key).getAsync();
        }
        BatchResult<?> batchResult = batch.execute();
        if (batchResult.getResponses().size() != keys.size()) {
            throw new IllegalStateException("batchResult.getResponses().size() != keys.size()");
        }
        for (int i = 0; i < keys.size(); i++) {
            System.out.println(keys.get(i) + "=" + batchResult.getResponses().get(i));
        }
    }
}
