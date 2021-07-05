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
     *指定word count 默认文件路径.
     */
    public static final String WORD_COUNT_PATH = "/flink1124/word.txt";
    /**
     * 定义HDFS的文件路径前缀.
     */
    public static final String HDFS_PATH = "hdfs://h7:9820/wordcount/output_";
    public static final String FILE_PATH = "filePath";

    /**
     * 定义私有构造方法.
     */
    private ConstantsWithPath() {
    }
}
