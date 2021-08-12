package com.flink.apps.source;

import cn.hutool.core.date.DatePattern;
import cn.hutool.core.date.DateUtil;
import com.flink.apps.constant.ConstantsWithPath;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Date;

/**
 * 从文件中读取数据
 *
 * @author ky2009666
 * @date 2021/8/9
 **/
public class FlinkDataSourceDemoFile02 {
    public static void main(String[] args) throws Exception {
        //E
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        //S
        DataStreamSource<String> f1Data = env.readTextFile(ConstantsWithPath.HELLO_PATH);
        String currentDate = DateUtil.format(new Date(), DatePattern.NORM_DATE_PATTERN);
        //DataStreamSource<String> f2HdfsData = env.readTextFile(ConstantsWithPath.HDFS_PATH + currentDate);
        DataStreamSource<String> f3ZipData = env.readTextFile(ConstantsWithPath.HELLO_PATH_ZIP);
        //T
        //TODO NONE
        //S
        f1Data.print("打印文本文件内容:");
        f3ZipData.print("打印压缩文件内容:");
        //E
        env.execute();
    }
}
