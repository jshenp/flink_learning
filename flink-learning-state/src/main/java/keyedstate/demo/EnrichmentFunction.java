package keyedstate.demo;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

public class EnrichmentFunction extends RichCoFlatMapFunction<OrderInfo1, OrderInfo2, Tuple2<OrderInfo1, OrderInfo2>> {

    private ValueState<OrderInfo1> orderInfo1State;
    private ValueState<OrderInfo2> orderInfo2State;

    @Override
    public void open(Configuration parameters) throws Exception {
        orderInfo1State = getRuntimeContext()
                .getState(new ValueStateDescriptor<OrderInfo1>("info1", OrderInfo1.class));
        orderInfo2State = getRuntimeContext()
                .getState(new ValueStateDescriptor<OrderInfo2>("info2", OrderInfo2.class));
    }

    @Override
    public void flatMap1(OrderInfo1 value, Collector<Tuple2<OrderInfo1, OrderInfo2>> out) throws Exception {
        OrderInfo2 order2 = orderInfo2State.value();
        if (order2 != null) {
            orderInfo2State.clear();
            out.collect(Tuple2.of(value, order2));
        } else {
            orderInfo1State.update(value);
        }
    }

    @Override
    public void flatMap2(OrderInfo2 value, Collector<Tuple2<OrderInfo1, OrderInfo2>> out) throws Exception {
        OrderInfo1 order1 = orderInfo1State.value();
        if (null != order1) {
            orderInfo1State.clear();
            out.collect(Tuple2.of(order1, value));
        } else {
            orderInfo2State.update(value);
        }
    }
}
