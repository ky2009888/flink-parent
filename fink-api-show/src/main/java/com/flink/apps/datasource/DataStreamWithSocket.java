package com.flink.apps.datasource;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 接收socket的文件内容
 *
 * @author root
 * @date 2021/7/6
 **/
public class DataStreamWithSocket {
    public static void main(String[] args) throws Exception {
        //E 建立流环境
        StreamExecutionEnvironment envStream = StreamExecutionEnvironment.getExecutionEnvironment();
        envStream.setParallelism(1);
        envStream.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        //S
        DataStreamSource<String> socketTextStream = envStream.socketTextStream("h7", 8899);
        //T
        //S
        socketTextStream.print();
        //E
        envStream.execute();
    }
}
