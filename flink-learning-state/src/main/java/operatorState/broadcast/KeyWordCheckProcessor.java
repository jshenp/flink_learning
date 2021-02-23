package operatorState.broadcast;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Objects;

/**
 * BroadCastState
 * 双流
 * Broadcast元素会分发到每个Task里
 * 实时修改控制值
 */
public class KeyWordCheckProcessor extends BroadcastProcessFunction<String, Tuple2<String, String>, String> {

    MapStateDescriptor<String, String> descriptor =
            new MapStateDescriptor<String, String>(
                    "ControlStream",
                    String.class,
                    String.class
            );

    @Override
    public void processElement(String value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
        String keywords = ctx.getBroadcastState(descriptor).get("key");
//        System.out.println("bd->" + keywords);
        if (!Objects.isNull(keywords)) {
//            System.out.println(value + " and " + keywords);
            if (value.contains(keywords)) {
                out.collect(value);
            }
        }

    }

    @Override
    public void processBroadcastElement(Tuple2<String, String> value, Context ctx, Collector<String> out) throws Exception {
        // 将接受到的数据放到BroadCast中
        ctx.getBroadcastState(descriptor).put(value.f0, value.f1);
        // 打印数据
//        System.out.println(Thread.currentThread().getName() + "BroadInfo :" + value);
    }
}
