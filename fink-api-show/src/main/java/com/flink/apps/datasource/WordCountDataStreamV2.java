package com.flink.apps.datasource;

import com.flink.apps.constant.ConstantsWithStringLable;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 单词统计通过流模式
 *
 * @author ky2009666
 * @date 2021/8/7
 **/
public class WordCountDataStreamV2 {
    public static void main(String[] args) throws Exception {
        //E 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度.
        env.setParallelism(1);
        //设置执行模式.
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        //S 获取数据
        DataStreamSource<String> dataStreamSource = env.fromElements(ConstantsWithStringLable.WORD_COUNT_CONTENT_STR);
        //T 进行数据切割成元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordRichMapData = dataStreamSource.flatMap(new RichFlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                //将单词进行拆分
                String[] words = line.split(" ");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        });
        //进行数据统计
        SingleOutputStreamOperator<Tuple2<String, Integer>> rusult = wordRichMapData.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        }).sum(1);
        //S 打印结果
        rusult.print("计算结果:");
        //E 启动执行环境
        env.execute();
    }
}
