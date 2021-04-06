package org.seepure.localcache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.concurrent.TimeUnit;

public class CaffeineBasic {

    public static void main(String[] args) {
        Cache<String, String> cache = Caffeine.newBuilder().maximumSize(50_000)
                .expireAfterWrite(1, TimeUnit.HOURS)
                .refreshAfterWrite(10, TimeUnit.MINUTES)
                .build();
    }

    public static String queryDB(String key) {
        return null;
    }

}
