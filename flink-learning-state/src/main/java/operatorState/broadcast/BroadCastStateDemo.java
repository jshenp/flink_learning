package operatorState.broadcast;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class BroadCastStateDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // BroadCast源
        DataStreamSource<String> broadCastSource = env.socketTextStream("123.56.68.23", 9999);

        // 数据源
        DataStreamSource<String> dataStreamSource = env.socketTextStream("123.56.68.23", 8889);

        // BroadCast流
        SingleOutputStreamOperator<Tuple2<String, String>> broadCastStream = broadCastSource.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String line) throws Exception {
                String[] split = line.split(",");
                return Tuple2.of(split[0], split[1]);
            }
        });


        //broadcast控制流里面的数据
        MapStateDescriptor<String, String> descriptor = new MapStateDescriptor<String, String>(
                "ControlStream",
                String.class,
                String.class
        );

        BroadcastStream<Tuple2<String, String>> broadcast = broadCastStream.broadcast(descriptor);

        dataStreamSource.connect(broadcast)
                .process(new KeyWordCheckProcessor())
                .print();

        env.execute("BroadCastState");
    }
}
