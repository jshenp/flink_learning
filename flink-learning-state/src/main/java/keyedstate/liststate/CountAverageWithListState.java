package keyedstate.liststate;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;

public class CountAverageWithListState extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Double>> {

    private ListState<Tuple2<Long, Long>> listState;

    @Override
    public void open(Configuration parameters) throws Exception {

        ListStateDescriptor<Tuple2<Long, Long>> listDis = new ListStateDescriptor<>("avg", TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {
        }));

        listState = getRuntimeContext().getListState(listDis);
    }

    @Override
    public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, Double>> out) throws Exception {

        Iterable<Tuple2<Long, Long>> state = listState.get();

        if (null == state) {
            listState.addAll(Collections.EMPTY_LIST);
        }

        listState.add(value);

        ArrayList<Tuple2<Long, Long>> datas = Lists.newArrayList(listState.get());
        // 满足条件
        if (datas.size() >= 3) {
            Long count = 0L;
            Long sum = 0L;
            for (Tuple2<Long, Long> data : datas) {
                count ++;
                sum += data.f1;
            }

            Double res = Double.valueOf(sum) / Double.valueOf(count);
            out.collect(Tuple2.of(value.f0, res));

            listState.clear();
        }
    }
}
