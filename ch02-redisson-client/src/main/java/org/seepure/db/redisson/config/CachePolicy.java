package org.seepure.db.redisson.config;

import java.util.Objects;
import java.util.stream.Stream;

public class CachePolicy {

    private String type;
    private int size;
    private boolean loadOnBeginning = false;
    private long expireAfterWrite;

    public CachePolicy() {
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public boolean isLoadOnBeginning() {
        return loadOnBeginning;
    }

    public void setLoadOnBeginning(boolean loadOnBeginning) {
        this.loadOnBeginning = loadOnBeginning;
    }

    public long getExpireAfterWrite() {
        return expireAfterWrite;
    }

    public void setExpireAfterWrite(long expireAfterWrite) {
        this.expireAfterWrite = expireAfterWrite;
    }

    public enum DimUpdatePolicy {
        MINUTE(15, 15),
        HOUR(300, 300),
        DAY(1800, 1800),
        RANDOM(-1, -1)
        ;

        public final int expireDuration;
        public final int refreshDuration;

        DimUpdatePolicy(int expireDuration, int refreshDuration) {
            this.expireDuration = expireDuration;
            this.refreshDuration = refreshDuration;
        }

        public static DimUpdatePolicy matches(String name) {
            return Stream.of(DimUpdatePolicy.values())
                    .filter(e -> Objects.equals(e.name(), name)).findAny().orElse(null);
        }
    }

}
