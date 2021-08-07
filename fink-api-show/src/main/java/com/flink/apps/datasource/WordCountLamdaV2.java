package com.flink.apps.datasource;

import com.flink.apps.constant.ConstantsWithStringLable;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 单词统计的lambda表达式版本
 *
 * @author ky2009666
 * @date 2021/8/7
 **/
public class WordCountLamdaV2 {
    public static void main(String[] args) throws Exception {
        //E 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);
        //S
        DataStreamSource<String> wordData = env.fromElements(ConstantsWithStringLable.WORD_COUNT_CONTENT_STR);
        //T
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = wordData.flatMap(
                (String line, Collector<Tuple2<String, Integer>> out) -> {
                    String[] words = line.split(ConstantsWithStringLable.SPACE_STRING);
                    for (String word : words) {
                        if (StringUtils.isNotBlank(word)) {
                            out.collect(Tuple2.of(word, 1));
                        }
                    }
                }
        ).returns(Types.TUPLE(Types.STRING, Types.INT)).keyBy((wordTube) -> wordTube.f0).sum(1);
        //S
        result.print();
        //E
        env.execute();
    }
}
