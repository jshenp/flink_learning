package keyedstate.mapstate;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * MapState:为每一个key保存一个Map集合
 * put():将对应的key的键值对放到状态中
 * values():拿到MapState中所有的value
 * clear():清空状态
 */
public class MapStateDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> stream = env.socketTextStream("123.56.68.23", 8889);

        SingleOutputStreamOperator<Tuple2<Long, Long>> dataStream = stream.map(new MapFunction<String, Tuple2<Long, Long>>() {

            @Override
            public Tuple2<Long, Long> map(String value) throws Exception {
                String[] split = value.split(",");
                return Tuple2.of(Long.valueOf(split[0]), Long.valueOf(split[1]));
            }
        });


        dataStream.keyBy(value -> value.f0).
                flatMap(new CountAverageWithMapState())
                .print();

        env.execute("ListStateDemo");

    }

}
