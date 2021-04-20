package org.seepure.localcache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * refreshAfterWrite并不像预想中的一样会有Schedule-Thread-Pool在后台去Kick-off Refresh, 而是要到下次cache.get的方法调用的时候才会将标记过期的entry给重新查
 */
public class CaffeineExpirePolicyTest {

    private static final ConcurrentHashMap<String, String> DB = new ConcurrentHashMap<>();

    public static void main(String[] args) throws InterruptedException {
        Cache<String, String> cache = Caffeine.newBuilder().maximumSize(20)
                .expireAfterWrite(20, TimeUnit.SECONDS)
                .refreshAfterWrite(10, TimeUnit.SECONDS)
                .build(new CacheLoader<String, String>() {
                    @Nullable
                    @Override
                    public String load(String s) throws Exception {
                        return queryDB(s);
                    }
                });
        int bound = 10;
        int interval = 10_000;
        UpdateHandler updateHandler = new UpdateHandler(bound, interval * 6);
        JoinHandler joinHandler = new JoinHandler(cache, bound, interval);

        new Thread(updateHandler).start();
        Thread.sleep(1000);
        new Thread(joinHandler).start();

    }

    public static String queryDB(String key) {
        return DB.get(key);
    }

    public static void writeDB(String key, String value) {
        DB.put(key, value);
    }

    private static class UpdateHandler implements Runnable {

        private static final Logger LOG = LoggerFactory.getLogger(UpdateHandler.class);
        private volatile boolean running = true;
        private int bound;
        private long interval;

        public UpdateHandler(int bound, long interval) {
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
                        writeDB(key, value);
                        LOG.info(String.format("write key: %s with value: %s to db", key, value));
                    }
                    Thread.sleep(interval);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static class JoinHandler implements Runnable {

        private static final Logger LOG = LoggerFactory.getLogger(JoinHandler.class);
        private volatile boolean running = true;
        private Cache<String, String> cache;
        private int bound;
        private long interval;

        public JoinHandler(Cache<String, String> cache, int bound, long interval) {
            this.cache = cache;
            this.bound = bound;
            this.interval = interval;
        }

        @Override
        public void run() {
            while (running) {
                try {
                    for (int i = 0; i < bound; i++) {
                        String key = "kk_" + i;
                        if (cache != null) {
                            String value = cache.getIfPresent(key);

                            if (value == null) { //do query db logic
                                String o = queryDB(key);
                                value = o == null ? "" : o;
                                cache.put(key, value);
                                LOG.info(String.format("query key: %s with value: %s from db", key, value));
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
