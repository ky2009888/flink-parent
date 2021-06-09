package com.flink.apps.sql;

import com.flink.apps.window.WordInfo;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author ky2009666
 * @description 通过flink SQL 进行单词统计
 * @date 2021/6/6
 **/
public class FlinkTableApiWordCount {
    /**
     * 定义文件路径.
     */
    public static final String FILE_PATH = "F:\\workspace-flink-2021\\flink-parent\\fink-api-show\\src\\main\\resources\\hello.txt";

    public static void main(String[] args) throws Exception {
        //获取flink的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(1);
        //进行环境设置
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        //获取table SQL的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        //获取文件输入流
        DataStreamSource<String> dataStreamSource = env.readTextFile(FILE_PATH);
        SingleOutputStreamOperator<WordInfo> wordsFlatMap = dataStreamSource.flatMap(new FlatMapFunction<String, WordInfo>() {
            @Override
            public void flatMap(String line, Collector<WordInfo> collector) throws Exception {
                String[] words = line.split(" ");
                for (String word :words) {
                    collector.collect(new WordInfo(word, 1));
                }
            }
        });
        tableEnv.createTemporaryView("wordA", wordsFlatMap, $("wordStr"), $("countWords"));
        Table tableCountResult = tableEnv.sqlQuery(" select wordStr,sum(countWords) as countWords from wordA group by wordStr  ");
        DataStream<Tuple2<Boolean, WordInfo>> tuple2DataStream = tableEnv.toRetractStream(tableCountResult, WordInfo.class);
        tuple2DataStream.print();
        env.execute();
    }

}
