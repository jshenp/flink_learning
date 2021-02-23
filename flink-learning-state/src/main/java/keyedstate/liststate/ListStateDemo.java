package keyedstate.liststate;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * ListState 顾名思义
 * get()
 * add()
 * clear
 */
public class ListStateDemo {

    /**
     * 实现每3个数进行求平均数
     */
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> stream = env.socketTextStream("123.56.68.23", 8889);


        stream.map(new MapFunction<String, Tuple2<Long, Long>>() {
            @Override
            public Tuple2<Long, Long> map(String value) throws Exception {
                String[] split = value.split(",");
                return new Tuple2(Long.valueOf(split[0]), Long.valueOf(split[1]));
            }
        }).keyBy(value -> value.f0)
                .flatMap(new CountAverageWithListState())
                .print();


        env.execute("liststatedemo");
    }
}
