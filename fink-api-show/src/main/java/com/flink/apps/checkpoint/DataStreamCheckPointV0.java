package com.flink.apps.checkpoint;

import com.flink.apps.constant.ConstantsWithPath;
import com.flink.apps.constant.ConstantsWithStringLable;
import org.apache.commons.lang3.SystemUtils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;

/**
 * flink 检查点功能演示
 *
 * @author root
 * @date 2021/7/8
 **/
public class DataStreamCheckPointV0 {

    /**
     * 默认执行路径
     *
     * @param args 命令行参数.
     */
    public static void main(String[] args) throws Exception {
        //E
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //这个代表出现异常就关闭应用程序，不做重启，如果不配置就代表无限重启
        env.setRestartStrategy(RestartStrategies.noRestart());
        //每2秒钟进行一次checkpoint
        env.enableCheckpointing(2000);
        // 设置模式为精确一次 (这是默认值)
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 确认 checkpoints 之间的时间会进行 500 ms
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        //设置可容忍检查点失败的数量
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(10);
        // Checkpoint 必须在一分钟内完成，否则就会被抛弃
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // 同一时间只允许一个checkpoint 进行
        //env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 开启在 job 中止后仍然保留的 externalized checkpoints
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //checkpoint 状态保存位置设定 windows 系统指定存储目录
        if (SystemUtils.IS_OS_WINDOWS) {
            env.setStateBackend(new FsStateBackend(ConstantsWithPath.WINDOW_CHECKPOINT_FILE_URI));
        } else {
            //其他操作系统下面，默认存储到hdfs路径
            env.setStateBackend(new FsStateBackend(ConstantsWithPath.HDFS_CHECKPOINT_DATA_URI));
        }
        //S
        DataStreamSource<String> socketTextStream
                = env.socketTextStream(ConstantsWithStringLable.H7_SERVER, ConstantsWithStringLable.H7_SERVER_PORT);
        //T
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultStream = socketTextStream.flatMap(new RichFlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                collector.collect(Tuple2.of(line.split(",")[0].trim(), Integer.parseInt(line.split(",")[1].trim())));
            }
        }).keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.f0;
            }
        }).sum(1);
        SingleOutputStreamOperator<String> map = resultStream.map(new RichMapFunction<Tuple2<String, Integer>, String>() {
            @Override
            public String map(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.f0 + ":<----->:" + stringIntegerTuple2.f1;
            }
        });
        map.print();
        //S
        map.addSink(
                new FlinkKafkaProducer<>(
                        ConstantsWithStringLable.H7_9092,
                        ConstantsWithStringLable.OUTPUT_KAFKA,
                        new SimpleStringSchema()));
        //E
        env.execute();
    }
}
