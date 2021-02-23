package com.jshenp.batch;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;

/**
 * OuterJoinDemo
 */
public class OuterJoinDemp {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //tuple2<用户id，用户姓名>
        ArrayList<Tuple2<Integer, String>> data1 = new ArrayList<>();
        data1.add(new Tuple2<>(1, "zs"));
        data1.add(new Tuple2<>(2, "ls"));
        data1.add(new Tuple2<>(3, "ww"));

        //tuple2<用户id，用户所在城市>
        ArrayList<Tuple2<Integer, String>> data2 = new ArrayList<>();
        data2.add(new Tuple2<>(1, "beijing"));
        data2.add(new Tuple2<>(2, "shanghai"));
        data2.add(new Tuple2<>(4, "guangzhou"));

        DataSource<Tuple2<Integer, String>> dataSet1 = env.fromCollection(data1);
        DataSource<Tuple2<Integer, String>> dataSet2 = env.fromCollection(data2);

        // left join
        JoinOperator<Tuple2<Integer, String>, Tuple2<Integer, String>, Object> leftJoinDataSet = dataSet1.leftOuterJoin(dataSet2)
                .where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Object>() {
                    @Override
                    public Object join(Tuple2<Integer, String> left, Tuple2<Integer, String> right) throws Exception {
                        // 如果右边为空
                        if (right == null) {
                            return new Tuple3<>(left.f0, left.f1, "NULL");
                        } else {
                            return new Tuple3<>(left.f0, left.f1, right.f1);
                        }
                    }
                });
        leftJoinDataSet.print();

        // right join
        JoinOperator<Tuple2<Integer, String>, Tuple2<Integer, String>, Object> joinDataSet2 = dataSet1.rightOuterJoin(dataSet2)
                .where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Object>() {
                    @Override
                    public Object join(Tuple2<Integer, String> left, Tuple2<Integer, String> right) throws Exception {
                        if (left == null) {
                            return new Tuple3<>(right.f0, right.f1, "NULL");
                        } else {
                            return new Tuple3<>(right.f0, right.f1, left.f1);
                        }
                    }
                });
        joinDataSet2.print();
    }
}
