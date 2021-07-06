package com.flink.apps.constant;

/**
 * 定义各种文件的路径
 *
 * @author ky2009666
 * @date 2021/7/4
 **/
public class ConstantsWithPath {
    /**
     * 定义word count的hello 文件的路径常量.
     */
    public static final String HELLO_PATH = "F:\\workspace-flink-2021\\flink-parent\\fink-api-show\\src\\main\\resources\\hello.txt";
    /**
     * 定义HDFS的路径.
     */
    public static final String HDFS_PATH = "hdfs://h7:9820/wordcount/output_";

    /**
     * 定义私有构造方法.
     */
    private ConstantsWithPath() {
    }
}
