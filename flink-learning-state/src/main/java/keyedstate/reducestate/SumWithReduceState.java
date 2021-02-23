package keyedstate.reducestate;

import akka.stream.impl.ReducerState;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class SumWithReduceState extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

    private ReducingState<Long> reducerState;

    @Override
    public void open(Configuration parameters) throws Exception { // 注册状态


        ReducingStateDescriptor<Long> descriptor =
                new ReducingStateDescriptor<Long>(
                        "sum",
                        new ReduceFunction<Long>() {
                            @Override
                            public Long reduce(Long value1, Long value2) throws Exception {
                                return value1 + value2;
                            }
                        }, Long.class);

        reducerState = getRuntimeContext().getReducingState(descriptor);
    }

    @Override
    public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, Long>> out) throws Exception {
        reducerState.add(value.f1);
        out.collect(Tuple2.of(value.f0, reducerState.get()));
    }
}
