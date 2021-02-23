package demo1;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import java.util.Properties;

/**
 * 通过socket读取数据存放到redis
 * checkpint主要是"容错"
 * 还有一个savepoint主要是"维护"，需要手动去处理，是一种特殊的checkpoint
 * 重启策略和checkpoint为flink的高可用提供的保障
 */
public class SinkForRedis {
    public static void main(String[] args) throws Exception {

        // TODO 重启策略的设置
        // 无
        // 固定次数
        // 失败率

        // TODO checkpoint设置
        // step1 获取程序入口
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // step2 开启checkpoint
        env.enableCheckpointing(5000);

        // step3 设置消费语意  --EXACTLY_ONCE--
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // step4 设置checkpoint的机制  任务取消保留ck
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);

        // step5 设置checkpoint的存储
        env.setStateBackend(new FsStateBackend("file:///Users/ps/Desktop/flink_ckeckpoint/flink_learning"));

        // step6 从kafka获取数据
        String topic = "test";
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "devbox3:9092");
        properties.setProperty("group.id", "jshenp_test_001");
        properties.setProperty("auto.offset.reset", "earliest");
        properties.setProperty("enable.auto.commit", "false");

        // step7 生成kafka comsumer
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        // 是否提交offset到kafka的comsumer_offset
        kafkaConsumer.setCommitOffsetsOnCheckpoints(true);
        DataStreamSource<String> data = env.addSource(kafkaConsumer).setParallelism(4);

        SingleOutputStreamOperator<Tuple2<String, String>> jshenp = data.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String s) throws Exception {
                System.out.println("kafka数据:" + s);
                return new Tuple2<>("b", s);
            }
        });

//        jshenp.print();

        FlinkJedisPoolConfig redisConf = new FlinkJedisPoolConfig.Builder()
                .setHost("123.56.68.23")
                .setPort(6379).build();

        RedisSink<Tuple2<String, String>> redisSink = new RedisSink<>(redisConf, new MyRedisMapper());

        jshenp.addSink(redisSink);

        env.execute(SinkForRedis.class.getSimpleName());
    }

    public static class MyRedisMapper implements RedisMapper<Tuple2<String, String>> {

        @Override
        public RedisCommandDescription getCommandDescription() {
            // LPUSH 从list的左边开始写数据，list的头会去右边
            return new RedisCommandDescription(RedisCommand.LPUSH
            );
        }

        @Override
        public String getKeyFromData(Tuple2<String, String> data) {
            return data.f0;
        }

        @Override
        public String getValueFromData(Tuple2<String, String> data) {
            return data.f1;
        }
    }
}
