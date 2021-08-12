package com.flink.apps.sink;

import com.flink.apps.constant.ConstantsWithPath;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink的sink类型
 *
 * @author ky2009666
 * @date 2021/8/10
 **/
public class FlinkSinkDemo01 {

    /**
     * 命令行入口方法
     *
     * @param args 命令行参数
     */
    public static void main(String[] args) throws Exception {
        //E
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        //S
        DataStreamSource<Long> numData = env.fromSequence(1, 30);
        //T
        //S
        numData.print("打印数字");
        numData.printToErr("打印数字红色:");
        numData.writeAsText(ConstantsWithPath.NUM_TXT_PATH);
        //E
        env.execute("执行任务");
    }
}
