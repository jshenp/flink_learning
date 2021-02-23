package keyedstate.demo;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class OrderStreamDemo {

    public static void main(String[] args) throws Exception {
//获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //读取数据
        DataStreamSource<String> info1 = env.addSource(new FileSource("/Users/ps/Desktop/jshenp/myselfproject/flink_learning/flink-learning-state/src/main/java/keyedstate/demo/OrderInfo1.txt"));
        DataStreamSource<String> info2 = env.addSource(new FileSource("/Users/ps/Desktop/jshenp/myselfproject/flink_learning/flink-learning-state/src/main/java/keyedstate/demo/OrderInfo2.txt"));

        KeyedStream<OrderInfo1, Long> order1 = info1.map(new MapFunction<String, OrderInfo1>() {
            @Override
            public OrderInfo1 map(String value) throws Exception {
                return OrderInfo1.string2OrderInfo1(value);
            }
        }).keyBy(order -> order.getOrderId());

        KeyedStream<OrderInfo2, Long> order2 = info2.map(new MapFunction<String, OrderInfo2>() {
            @Override
            public OrderInfo2 map(String value) throws Exception {
                return OrderInfo2.string2OrderInfo2(value);
            }
        }).keyBy(order -> order.getOrderId());

        order1.connect(order2)
                .flatMap(new EnrichmentFunction())
                .print();

        env.execute("StateDemo");
    }


}
