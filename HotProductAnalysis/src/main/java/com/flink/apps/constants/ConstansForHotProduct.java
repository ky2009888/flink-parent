package com.flink.apps.constants;

/**
 * 热门商品相关的常量
 *
 * @author ky2009666
 * @date 2021/7/11
 **/
public class ConstansForHotProduct {
    /**
     * 热门商品的数据的路径.
     */
    public static final String RESOURCES_USER_BEHAVIOR_CSV = "F:\\workspace-flink-2021\\flink-parent\\HotProductAnalysis\\src\\main\\resources\\UserBehavior.csv";
    /**
     * 英文逗号.
     */
    public static final String COMMA = ",";
    /**
     * "pv".
     */
    public static final String PV = "pv";
    /**
     * 热门商品的数据的路径-Linux.
     */
    public static final String RESOURCES_USER_BEHAVIOR_CSV_LINUX = "/work/workspace-flink-learn/flink-parent/HotProductAnalysis/src/main/resources/UserBehavior.csv";
    /**
     * 热门商品的数据的路径-Linux.
     */
    public static final String RESOURCES_USER_BEHAVIOR_CSV_LINUX_1 = "/work/workspace-flink-learn/flink-parent/HotProductAnalysis/src/main/resources/UserBehavior1.csv";
    /**
     * bootstrap.servers string.
     */
    public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
    /**
     * kafka server IP:PORT.
     */
    public static final String H7_9092 = "h7:9092";
    /**
     * h7 server.
     */
    public static final String H7_SERVER = "h7";
    /**
     * h7 server port.
     */
    public static final Integer H7_SERVER_PORT = 9999;
    /**
     * kafka group id.
     */
    public static final String GROUP_ID = "group.id";
    /**
     * consumer group name.
     */
    public static final String TEST_CONSUMER_GROUP = "test-consumer-group";
    /**
     * input_kafka String.
     */
    public static final String INPUT_KAFKA = "input_kafka";
    /**
     * input_kafka String.
     */
    public static final String HOT_PRODUCT_REL = "hot_product_rel";
    /**
     * auto.offset.reset. HOT_PRODUCT_REL
     */
    public static final String AUTO_OFFSET_RESET = "auto.offset.reset";
    /**
     * latest.
     */
    public static final String LATEST = "latest";
    /**
     * flink.partition-discovery.interval-millis.
     */
    public static final String FLINK_PARTITION_DISCOVERY_INTERVAL_MILLIS = "flink.partition-discovery.interval-millis";
    /**
     * "5000".
     */
    public static final String FIVE_THOUSAND = "5000";
    /**
     * enable.auto.commit.
     */
    public static final String ENABLE_AUTO_COMMIT = "enable.auto.commit";
    /**
     * true.
     */
    public static final String TRUE = "true";
    /**
     * auto.commit.interval.ms.
     */
    public static final String AUTO_COMMIT_INTERVAL_MS = "auto.commit.interval.ms";
    /**
     * "2000".
     */
    public static final String TWO_THOUSAND = "2000";
    /**
     * output_kafka.
     */
    public static final String OUTPUT_KAFKA = "output_kafka";
}
