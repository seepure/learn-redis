package org.seepure.db.redisson.config;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.ClusterServersConfig;
import org.redisson.config.Config;
import org.redisson.config.MasterSlaveServersConfig;
import org.seepure.db.redisson.config.CachePolicy.DimUpdatePolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigUtil {

    private static final Logger LOG = LoggerFactory.getLogger(ConfigUtil.class);

    public static Map<String, String> getArgMapFromArgs(String arg) {
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

    public static Config buildRedissonConfig(Map<String, String> configMap) {
        String mode = configMap.get("redis.mode");
        if (StringUtils.isBlank(mode)) {
            mode = configMap.getOrDefault("mode", "").toLowerCase();
        }
        String nodes = configMap.get("redis.nodes");
        if (StringUtils.isBlank(nodes)) {
            nodes = configMap.get("nodes");
        }
        String password = configMap.get("redis.auth");
        if (password == null) {
            password = configMap.get("auth");
        }
        Config config = new Config();
        config.setCodec(StringCodec.INSTANCE);
        switch (mode) {
            case "cluster":
                ClusterServersConfig clusterServersConfig = config.useClusterServers();
                String[] addresses = nodes.split(",");
                if (addresses == null || addresses.length < 1) {
                    throw new IllegalArgumentException("empty config nodes for redis.");
                }
                for (int i=0; i < addresses.length; i++) {
                    if (!addresses[i].startsWith("redis://")) {
                        addresses[i] = "redis://" + addresses[i];
                    }
                }
                clusterServersConfig.addNodeAddress(addresses);
                if (StringUtils.isNotBlank(password)) {
                    clusterServersConfig.setPassword(password);
                }
                break;
            default:
                MasterSlaveServersConfig masterSlaveServersConfig = config.useMasterSlaveServers();
                if (StringUtils.isBlank(nodes)) {
                    throw new IllegalArgumentException("empty config nodes for redis.");
                }
                if (!nodes.startsWith("redis://")) {
                    nodes = "redis://" + nodes;
                }
                masterSlaveServersConfig.setMasterAddress(nodes);
                if (StringUtils.isNotBlank(password)) {
                    masterSlaveServersConfig.setPassword(password);
                }
                break;
        }
        return config;
    }

    public static CachePolicy getCachePolicy(Map<String, String> configMap) {
        CachePolicy cachePolicy = new CachePolicy();
        String type = configMap.get("cachePolicy.type");
        if (!Objects.equals(type, "local")) {
            return null;
        }
        cachePolicy.setType(type);
        String expireAfterWrite = configMap.get("cachePolicy.expireAfterWrite");
        if (StringUtils.isNotBlank(expireAfterWrite)) {
            long ttl = Long.parseLong(expireAfterWrite);
            cachePolicy.setExpireAfterWrite(ttl);
        } else {
            String dimUpdateType = configMap.get("cachePolicy.dimUpdatePolicy");
            CachePolicy.DimUpdatePolicy dimUpdatePolicy = DimUpdatePolicy.matches(dimUpdateType.toUpperCase());
            if (dimUpdatePolicy == null || dimUpdatePolicy == DimUpdatePolicy.RANDOM) {
                return null;
            }
            cachePolicy.setExpireAfterWrite(dimUpdatePolicy.expireDuration);
        }

        String loadOnBeginning = configMap.getOrDefault("cachePolicy.loadOnBeginning", "false");
        String size = configMap.get("cachePolicy.size");

        cachePolicy.setSize(Integer.parseInt(size));
        cachePolicy.setLoadOnBeginning(Boolean.getBoolean(loadOnBeginning));
        return cachePolicy;
    }
}
