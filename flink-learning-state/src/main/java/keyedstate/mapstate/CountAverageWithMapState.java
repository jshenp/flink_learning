package keyedstate.mapstate;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.UUID;

public class CountAverageWithMapState extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Double>> {

    private MapState<String, Long> mapState;

    @Override
    public void open(Configuration parameters) throws Exception {
        MapStateDescriptor<String, Long> dis = new MapStateDescriptor<>("avg", String.class, Long.class);
        mapState = getRuntimeContext().getMapState(dis);
    }

    @Override
    public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, Double>> out) throws Exception {
        mapState.put(UUID.randomUUID().toString(), value.f1);

        ArrayList<Long> datas = Lists.newArrayList(mapState.values());
        if (datas.size() >= 3) {
            Long count = 0L;
            Long sum = 0L;
            for (Long data : datas) {
                count++;
                sum += data;
            }

            Long result = sum / count;
            out.collect(new Tuple2(value.f0, Double.valueOf(result)));
            mapState.clear();
        }
    }
}
