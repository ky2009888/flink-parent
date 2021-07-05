package com.flink.apps.dataset;

import com.flink.apps.constant.ConstantsWithPath;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 通过DataStream来演示word count案例ESTS
 *
 * @author ky2009666
 * @date 2021/7/4
 **/
public class WordCountWriteToHdfs {

    /**
     * 程序执行的入口方法.
     *
     * @param args 命令行参数.
     */
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置执行模式
        streamEnv.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        ParameterTool argsParTool = ParameterTool.fromArgs(args);
        //定义文件路径
        String filePath;
        if (argsParTool.has(ConstantsWithPath.FILE_PATH)) {
            filePath = argsParTool.get(ConstantsWithPath.FILE_PATH);
        } else {
            filePath = ConstantsWithPath.WORD_COUNT_PATH;
        }
        DataStreamSource<String> dataStreamSource =
                streamEnv.readTextFile(filePath);
        //设置操作用户
        System.setProperty("HADOOP_USER_NAME", "root");
        dataStreamSource
                //将一行文本根据空格进行分割成Tuple2，每个单词占一行
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        for (String word : line.split(" ")
                        ) {
                            collector.collect(Tuple2.of(word, 1));
                        }
                    }
                })
                //根据KEY 进行分组
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    /**
                     * User-defined function that deterministically extracts the key from an object.
                     *
                     * @param value The object to get the key from.
                     * @return The extracted key.
                     * @throws Exception Throwing an exception will cause the execution of the respective task to
                     *                   fail, and trigger recovery or cancellation of the program.
                     */
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.getField(0);
                    }
                })
                //进行计算
                .sum(1)
                .writeAsText(ConstantsWithPath.HDFS_PATH + System.currentTimeMillis())
                .setParallelism(1);
        //程序执行
        streamEnv.execute("Job-Word-Count-2021-07-04");
    }
}
