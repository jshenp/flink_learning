package com.jshenp.batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 * Flink核心编程模块
 * 1、Source
 * 2、Transform
 * 3、Sink
 */
public class WordCountDemo01 {

    public static void main(String[] args) throws Exception {
        // step1:获取离线程序入口
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ArrayList<String> data = new ArrayList<>();
        data.add("you,jump");
        data.add("i,jump");

        // step2:获取数据源
        DataSource<String> dataSet = env.fromCollection(data);
        // step3:数据处理
        AggregateOperator<Tuple2<String, Integer>> result = dataSet.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] splits = line.split(",");
                for (String str : splits) {
                    collector.collect(new Tuple2<>(str, 1));
                }
            }
        }).groupBy(0).sum(1);

        // step4:结果处理
        result.print();
        // step5:启动程序
//        env.execute("wc01");
    }
}
