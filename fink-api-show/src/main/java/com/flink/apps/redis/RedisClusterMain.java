package com.flink.apps.redis;

import com.flink.apps.constant.ConstantsWithStringLable;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.util.HashSet;
import java.util.Set;

/**
 * redis集群
 *
 * @author ky2009666
 * @date 2021/8/11
 **/
@Slf4j
public class RedisClusterMain {
    public static void main(String[] args) {
        Set<HostAndPort> set =new HashSet<>();
        set.add(new HostAndPort(ConstantsWithStringLable.H7_SERVER_ADDR,7001));
        set.add(new HostAndPort(ConstantsWithStringLable.H7_SERVER_ADDR,7002));
        set.add(new HostAndPort(ConstantsWithStringLable.H7_SERVER_ADDR,7003));
        set.add(new HostAndPort(ConstantsWithStringLable.H7_SERVER_ADDR,8001));
        set.add(new HostAndPort(ConstantsWithStringLable.H7_SERVER_ADDR,8002));
        set.add(new HostAndPort(ConstantsWithStringLable.H7_SERVER_ADDR,8003));
        JedisCluster jedisCluster=new JedisCluster(set);
        jedisCluster.set("k1", "v1");
        log.info(jedisCluster.get("k1"));
    }
}
