package com.flink.apps.dataset;

import com.flink.apps.constant.ConstantsWithPath;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 通过DataSet API进行数据操作----STS---SOURCE-TRANSLATE-SINK
 *
 * @author ky2009666
 * @date 2021/7/4
 **/
public class WordCountWithDataSetApi {

    /**
     * 命令行入口
     *
     * @param args 命令行参数.
     */
    public static void main(String[] args) throws Exception {
        sumTotalWords();
        totalCountByWord();
    }

    /**
     * 统计所有的单词个数.
     *
     * @throws Exception 异常.
     */
    private static void sumTotalWords() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> wordsDataSet =
                env.readTextFile(ConstantsWithPath.HELLO_PATH);
        wordsDataSet.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> collector) throws Exception {
                for (String word : line.split(" ")
                ) {
                    collector.collect(word);
                }
            }
        }).map(new MapFunction<String, Tuple1<Integer>>() {
            @Override
            public Tuple1<Integer> map(String word) throws Exception {
                return Tuple1.of(1);
            }
        }).sum(0).print();
    }

    /**
     * 统计每个单词出现的次数.
     *
     * @throws Exception 异常.
     */
    private static void totalCountByWord() throws Exception {
        //创建基础环境
        ExecutionEnvironment evn = ExecutionEnvironment.getExecutionEnvironment();
        //数据源 S SOURCE
        DataSet<String> dataSet = evn.fromElements("Who's there?",
                "I think I hear them. Stand, ho! Who's there?");
        //数据转换处理 T 切割->分组->聚合
        DataSet<Tuple2<String, Integer>> sumData = dataSet
                //进行数据的切割操作
                .flatMap(new LineHandlers())
                //进行数据分组
                .groupBy(0)
                //将每组的单词求和
                .sum(1);
        sumData.print();
    }

    /**
     * word处理类.
     */
    public static class LineHandlers implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>> out) {
            for (String word : line.split(" ")) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }
}
