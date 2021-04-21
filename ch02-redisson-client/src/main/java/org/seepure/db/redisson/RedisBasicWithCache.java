package org.seepure.db.redisson;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.redisson.Redisson;
import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.seepure.db.redisson.config.CachePolicy;
import org.seepure.db.redisson.config.ConfigUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisBasicWithCache {

    private static final Logger LOG = LoggerFactory.getLogger(RedisBasicWithCache.class);

    public static void main(String[] args) throws Exception {
        String arg = args != null && args.length >= 1 ? args[0] :
                "mode=cluster;nodes=redis://192.168.234.137:7000,redis://192.168.234.137:7001,redis://192.168.234.138:7000,redis://192.168.234.138:7001,redis://192.168.234.134:7000,redis://192.168.234.134:7001"
                        + ";cachePolicy.size=10;cachePolicy.dimUpdatePolicy=minute;cachePolicy.type=local";
        Map<String, String> configMap = ConfigUtil.getArgMapFromArgs(arg);
        Config config = ConfigUtil.buildRedissonConfig(configMap);
        CachePolicy cachePolicy = ConfigUtil.getCachePolicy(configMap);
        RedissonClient client = Redisson.create(config);
        int bound = 10;
        int interval = 10_000;
        UpdateHandler updateHandler = new UpdateHandler(client, bound, interval * 6);
        JoinHandler joinHandler = new JoinHandler(client, cachePolicy, bound, interval);

        new Thread(updateHandler).start();
        Thread.sleep(1000);
        new Thread(joinHandler).start();
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
                try {
                    for (int i = 0; i < bound; i++) {
                        String key = "kk_" + i;
                        String value = key + "&" + sdf.format(new Date());
                        RBucket<Object> bucket = client.getBucket(key);
                        bucket.set(value, 800, TimeUnit.SECONDS);
                        LOG.info(String.format("write key: %s with value: %s to redis", key, value));
                    }
                    Thread.sleep(interval);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        public void stop() {
            running = false;
        }
    }

    private static class JoinHandler implements Runnable {

        private static final Logger LOG = LoggerFactory.getLogger(JoinHandler.class);
        private Cache<String, String> cache;
        private volatile boolean running = true;
        private RedissonClient client;
        private CachePolicy cachePolicy;
        private int bound;
        private long interval;

        public JoinHandler(RedissonClient client, CachePolicy cachePolicy, int bound, long interval) {
            this.client = client;
            this.cachePolicy = cachePolicy;
            this.bound = bound;
            this.interval = interval;
            this.cache = buildCache(cachePolicy);
        }

        private Cache<String, String> buildCache(CachePolicy cachePolicy) {
            if (cachePolicy == null) {
                return null;
            }
            //final RedissonClient client = this.client;
            Cache<String, String> cache = Caffeine.newBuilder().maximumSize(cachePolicy.getSize())
                    .expireAfterWrite(cachePolicy.getExpireAfterWrite(), TimeUnit.SECONDS).build();
                    //refreshAfterWrite并不像预想中的一样会有Schedule-Thread-Pool在后台去Kick-off Refresh, 而是要到下次cache.get的方法调用的时候才会将标记过期的entry给重新查
                    //所以还不如直接用expireAfterWrite -- 这里的这个选项也应该放权给用户来设置
                    //.refreshAfterWrite(cachePolicy.getDimUpdatePolicy().refreshDuration, TimeUnit.SECONDS)
//                    .build(new CacheLoader<String, String>() {
//                        @Nullable
//                        @Override
//                        public String load(@NonNull String key) throws Exception {
//                            LOG.debug("loading key: " + key + " from redis begins.");
//                            RBucket<Object> bucket = client.getBucket(key);
//                            Object o = bucket.get();
//                            LOG.info("loading key: " + key + " from redis finished.");
//                            return o == null ? null : String.valueOf(o);
//                        }
//                    });
            return cache;
        }

        @Override
        public void run() {
            while (running) {
                try {
                    for (int i = 0; i < bound; i++) {
                        String key = "kk_" + i;
                        if (cache != null) {
                            String value = cache.getIfPresent(key);

                            if (value == null) { //do query redis logic
                                RBucket<Object> bucket = client.getBucket(key);
                                Object o = bucket.get();
                                value = o == null ? "" : o.toString();
                                cache.put(key, o == null ? "" : o.toString());
                                LOG.info(String.format("query key: %s with value: %s from redis", key, value));
                            } else {
                                LOG.info(String.format("read key: %s with value: %s from cache", key, value));
                            }
                        }
                    }
                    Thread.sleep(interval);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        public void stop() {
            running = false;
        }
    }
}
