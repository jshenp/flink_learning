package keyedstate.reducestate;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 聚合方法
 * 将相同key的值进行相加
 */
public class ReduceStateDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> stream = env.socketTextStream("123.56.68.23", 8889);

        stream.map(new MapFunction<String, Tuple2<Long, Long>>() {
            @Override
            public Tuple2<Long, Long> map(String line) throws Exception {
                String[] split = line.split(",");
                Long key = Long.valueOf(split[0]);
                Long value = Long.valueOf(split[1]);
                return Tuple2.of(key, value);
            }
        }).keyBy(value -> value.f0)
                .flatMap(new SumWithReduceState())
                .print();

        env.execute("ReducingStateDemo");
    }
}
