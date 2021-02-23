package com.jshenp.batch;

import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapPartitionOperator;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;

public class MapPartitionDemo {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ArrayList<String> data = new ArrayList<>();
        data.add("you,jump");
        data.add("i,jump");
        DataSource<String> dataSet = env.fromCollection(data);

        /**
         * MapPartition第一层循环一般用于做连接初始化，例如MySQL等
         */
        MapPartitionOperator<String, String> wordDataSet = dataSet.mapPartition(new MapPartitionFunction<String, String>() {
            @Override
            public void mapPartition(Iterable<String> iterable, Collector<String> collector) throws Exception {
                Iterator<String> iterator = iterable.iterator();
                while (iterator.hasNext()) {
                    String next = iterator.next();
                    String[] split = next.split(",");
                    for (String str : split) {
                        collector.collect(str);
                    }
                }
            }
        });

        wordDataSet.print();
    }
}
