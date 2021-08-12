package com.flink.apps.source;

import com.flink.apps.constant.ConstantsWithStringLable;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 从socket流中读取数据
 *
 * @author ky2009666
 * @date 2021/8/9
 **/
public class FlinkDataSourceDemoSocket03 {
    public static void main(String[] args) throws Exception {
        //E
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        //S
        DataStreamSource<String> socketTextStream = env.socketTextStream(ConstantsWithStringLable.H7_SERVER, ConstantsWithStringLable.H7_SERVER_PORT);
        //T
        //TODO NONE
        //S
        socketTextStream.print("打印socket流的内容:");
        //E
        env.execute();
    }
}
