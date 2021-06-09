package com.flink.apps.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @author ky2009666
 * @description 时间窗口案例
 * @date 2021/6/6
 **/
public class TimeWindowShowUsg {
    /**
     * 命令行执行入口.
     *
     * @param args
     */
    public static void main(String[] args) throws Exception {
        //获取flink流的执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置执行的并行度
        environment.setParallelism(4);
        //读取文件，source
        DataStreamSource<String> streamSource = environment.readTextFile("F:\\workspace-flink-2021\\flink-parent\\fink-api-show\\src\\main\\resources\\hello.txt");
        //进行处理
        SingleOutputStreamOperator<WordInfo> outputStreamOperator = streamSource.map(
                new MapFunction<String, WordInfo>() {
                    @Override
                    public WordInfo map(String line) throws Exception {
                        for (String word : line.split(" ")) {
                            return new WordInfo(word, 1);
                        }
                        return new WordInfo("", 0);
                    }
                }
        );
        outputStreamOperator.keyBy(new KeySelector<WordInfo, Object>() {
            @Override
            public Object getKey(WordInfo wordInfo) throws Exception {
                return wordInfo.getWordStr();
            }
        }).window(TumblingProcessingTimeWindows.of(Time.seconds(2))).sum("count").print();
        //环境执行
        environment.execute();
    }
}
