package keyedstate.valuestate;

import org.apache.flink.api.common.functions.MapFunction;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * valuesatet<T> 这个状态为每一个 key 保存一个值
 * 3个方法
 * value()获取状态值
 * update()更新状态值
 * clear()清除状态
 */
public class ValueStateDemo {

    /**
     * 简单实现每3个数求一次平均值
     *
     * @param args
     */
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> stream = env.socketTextStream("123.56.68.23", 8889);

        SingleOutputStreamOperator<Tuple2<Long, Long>> dataStream = stream.map(new MapFunction<String, Tuple2<Long, Long>>() {
            @Override
            public Tuple2<Long, Long> map(String value) throws Exception {
                Long keys = Long.valueOf(value.split(",")[0]);
                Long values = Long.valueOf(value.split(",")[1]);
                return new Tuple2(keys, values);
            }
        });

        dataStream.keyBy(value -> value.f0)
                .flatMap(new CountAverageWithValuesState())
                .print();

        env.execute("valueState");

    }
}
