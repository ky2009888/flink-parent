package com.flink.apps.datasource;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateUtil;
import com.flink.apps.constant.ConstantsWithPath;
import com.flink.apps.constant.ConstantsWithStringLable;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Date;

/**
 * 将单词统计的结果写入hdfs
 *
 * @author ky2009666
 * @date 2021/8/7
 **/
public class WordCountWriteToHdfsV2 {
    public static void main(String[] args) throws Exception {
        //E
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);
        //S
        DataStreamSource<String> words = env.fromElements(ConstantsWithStringLable.WORD_COUNT_CONTENT_STR);
        //T
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = words.flatMap((String line, Collector<Tuple2<String, Integer>> out) -> {
            String[] wordsArray = line.split(ConstantsWithStringLable.SPACE_STRING);
            for (String word : wordsArray) {
                out.collect(Tuple2.of(word, 1));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.INT)).keyBy((wordTuple2) -> wordTuple2.f0).sum(1);
        //S
        String currentDate = DateUtil.format(new Date(), DatePattern.PURE_TIME_PATTERN);
        result.writeAsText(ConstantsWithPath.HDFS_PATH+currentDate);
        //E
        env.execute("执行单词统计任务，将结果写入hdfs");
    }
}
