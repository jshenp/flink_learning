package keyedstate.valuestate;

import apple.laf.JRSUIState;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class CountAverageWithValuesState extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Double>> {

    // state保存的类型是(Long,Long)
    private ValueState<Tuple2<Long, Long>> countAndSumState;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 固定写法
        // 定义
        ValueStateDescriptor<Tuple2<Long, Long>> valueStateDescriptor = new ValueStateDescriptor<>("avgs", TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {
        }));

        // 初始化
        countAndSumState = getRuntimeContext().getState(valueStateDescriptor);
    }

    @Override
    public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, Double>> out) throws Exception {
        // 拿到当前的state值
        Tuple2<Long, Long> currentState = countAndSumState.value();

        // 如果当前的值没有就初始化
        if (null == currentState) {
            currentState = Tuple2.of(0L, 0L);
        }

        currentState.f0 += 1;
        currentState.f1 += value.f1;

        // 更新状态
        countAndSumState.update(currentState);

        // 判断是否满足3的条件
        if (currentState.f0 >= 3) {
            // 计算平均数
            double avg = currentState.f1 / currentState.f0;
            // 写出数据
            out.collect(Tuple2.of(value.f0, avg));
            // 清空state
            countAndSumState.clear();
        }

    }
}
