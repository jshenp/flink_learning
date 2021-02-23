package com.jshenp.batch;

import com.sun.xml.internal.rngom.digested.DValuePattern;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;

/**
 * 计数器Demo
 */
public class CounterDemo {

    public static void main(String[] args) throws Exception {
        //获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> data = env.fromElements("a", "b", "c", "d", "e", "f", "g");

        MapOperator<String, String> accumulatorDataSet = data.map(new RichMapFunction<String, String>() {

            // 1、创建累加器
            private IntCounter numLines = new IntCounter();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                // 2、注册累加器
                getRuntimeContext().addAccumulator("num-lines", numLines);
            }

            @Override
            public String map(String value) throws Exception {
                this.numLines.add(1);
                System.out.println("Map:" + numLines.getLocalValue());
                return value;
            }
        }).setParallelism(2);

        accumulatorDataSet.writeAsText("/Users/ps/Desktop/test/flink_output");

        JobExecutionResult jobResult = env.execute("test");
        int num = jobResult.getAccumulatorResult("num-lines");
        System.out.println(num);
    }
}
