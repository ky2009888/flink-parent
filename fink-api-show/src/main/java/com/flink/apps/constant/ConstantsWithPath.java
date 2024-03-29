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
     * 指定word count 默认文件路径.
     */
    public static final String WORD_COUNT_PATH = "/flink1124/word.txt";
    /**
     * 定义HDFS的文件路径前缀.
     */
    public static final String HDFS_PATH = "hdfs://h7:9820/wordcount/output_";
    public static final String FILE_PATH = "filePath";
    public static final String ROOT_RESULT_PATH = "/root/result1.txt";
    public static final String HDFS_CHECKPOINT_DATA_URI = "hdfs://192.168.64.7:9820/flink-checkpoint/checkpoint";
    /**
     * windows环境下面checkpoint的保存路径.
     */
    public static final String WINDOW_CHECKPOINT_FILE_URI = "file:///d:/ckp";
    public static final String HELLO_PATH_ZIP = "F:\\workspace-flink-2021\\flink-parent\\fink-api-show\\src\\main\\resources\\hello.zip";
    public static final String NUM_TXT_PATH = "F:\\workspace-flink-2021\\flink-parent\\fink-api-show\\src\\main\\resources\\num.txt";

    /**
     * 定义私有构造方法.
     */
    private ConstantsWithPath() {
    }
}
