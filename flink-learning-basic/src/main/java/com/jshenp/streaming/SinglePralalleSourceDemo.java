package com.jshenp.streaming;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SinglePralalleSourceDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(3);

        DataStreamSource<Long> source = env.addSource(new MySinglePralalleSource());

        SingleOutputStreamOperator<Long> dataStream = source.broadcast().map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long num) {
                System.out.println("线程id：" + Thread.currentThread().getId() + "接受到了数据：" + num);
                return num;
            }
        });

        SingleOutputStreamOperator<Long> filterDataStream = dataStream.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long num) {
                return num % 2 == 0;
            }
        });

        filterDataStream.print().setParallelism(1);

        env.execute("test");

    }

}
