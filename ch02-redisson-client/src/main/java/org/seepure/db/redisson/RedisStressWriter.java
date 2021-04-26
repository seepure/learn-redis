package org.seepure.db.redisson;

import org.redisson.Redisson;
import org.redisson.api.RMap;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.seepure.db.redisson.config.ConfigUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class RedisStressWriter {

    public static void main(String[] args) {
        String arg = args != null && args.length >= 1 ? args[0] :
                "mode=cluster;nodes=redis://192.168.234.137:7000,redis://192.168.234.137:7001,redis://192.168.234.138:7000,redis://192.168.234.138:7001,redis://192.168.234.134:7000,redis://192.168.234.134:7001";
        //"mode=cluster;nodes=redis://192.168.213.128:7000,redis://192.168.213.128:7001,redis://192.168.213.129:7000,redis://192.168.213.129:7001,redis://192.168.213.130:7000,redis://192.168.213.130:7001";
        Map<String, String> configMap = ConfigUtil.getArgMapFromArgs(arg);
        int bound = 100_000;
        int interval = 5;
        int threadNum = 4;
        Config config = ConfigUtil.buildRedissonConfig(configMap);
        RedissonClient client = Redisson.create(config);
        ExecutorService executorService = Executors.newFixedThreadPool(threadNum);
        for (int i=0; i < threadNum; i++) {
            executorService.execute(new UpdateHandler(client, bound, interval));
        }
    }

    private static class UpdateHandler implements Runnable {

        private static final Logger LOG = LoggerFactory.getLogger(UpdateHandler.class);
        private volatile boolean running = true;
        private RedissonClient client;
        private int bound;
        private long interval;

        public UpdateHandler(RedissonClient client, int bound, long interval) {
            this.client = client;
            this.bound = bound;
            this.interval = interval;
        }

        @Override
        public void run() {
            while (running) {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
                Random random = new Random();
                try {
                    for (int i = 0; i < 10; i++) {
                        int num = genNum(random, bound);
                        String key = "kh_" + num;
                        RMap<String, String> rMap = client.getMap(key);
                        LinkedHashMap<String, String> map = new LinkedHashMap<>();
                        map.put("id", String.valueOf(num));
                        map.put("update_time", sdf.format(new Date()));
                        rMap.putAll(map);
                        rMap.expire(900, TimeUnit.SECONDS);
                        //LOG.info(String.format("write key: %s with value: %s to redis", key, value));
                    }
                    Thread.sleep(interval);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        private int genNum(Random random, int bound) {
            int num = -1;
            int classNum = random.nextInt(100);
            if (classNum < 25) {
                num = random.nextInt(bound / 100);
            } else if (classNum < 40) {
                num = random.nextInt(bound * 5 / 100);
            } else if (classNum < 65) {
                num = random.nextInt(bound / 10);
            } else if (classNum < 90) {
                num = bound / 10 + random.nextInt(bound * 2 / 10);
            } else {
                num = bound * (1 + 2) / 10 + random.nextInt(bound * 7 / 10);
            }
            return num;
        }

        public void stop() {
            running = false;
        }
    }
}
