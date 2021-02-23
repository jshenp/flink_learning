package operatorState.operator;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * 自定义打印opeartor
 * operator是基于task
 * <p>
 * 要实现CheckpointedFunction或者ListCheckpointedFunction
 */
public class CustomSink implements SinkFunction<Tuple2<String, Integer>>, CheckpointedFunction {

    // 缓存数据结果
    private List<Tuple2<String, Integer>> bufferElements;
    // 内存中数据大小阀值
    private int threshold;
    // 保存内存中状态信息
    private ListState<Tuple2<String, Integer>> checkpointState;

    public CustomSink(int threshold) {
        this.threshold = threshold;
        this.bufferElements = new ArrayList<>();
    }

    @Override
    public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
        // 可以将接收到的每一条数据保存到任何的存储系统中
        bufferElements.add(value);
        if (bufferElements.size() == threshold) { // 2
            // 简单打印/实际场景应该是存储起来（batchsize）
            System.out.println("自定义格式：" + bufferElements);
            bufferElements.clear();
        }
    }

    /**
     * 快照方法
     * 用于将内存数据保存到状态中
     * 定义执行
     * 快照：将数据写到state
     * @param context
     * @throws Exception
     */
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        checkpointState.clear();
        for (Tuple2<String, Integer> ele : bufferElements) {
            checkpointState.add(ele);
        }
    }

    /**
     * 初始化state
     * 用于程序从状态中恢复数据到内存
     *
     * 初始化：将快照(state)数据写到内存
     *
     * @param context
     * @throws Exception
     */
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<Tuple2<String, Integer>> listState = new ListStateDescriptor<>("buffer-ele", TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
        }));

        checkpointState = context.getOperatorStateStore().getListState(listState);

        // 如果快照是从上一次返回的
        if (context.isRestored()) {
            for (Tuple2<String, Integer> ele : checkpointState.get()) {
                bufferElements.add(ele);
            }
        }
    }
}
