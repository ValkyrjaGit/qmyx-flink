package com.ytjj.qmyx.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ytjj.qmyx.utils.MyKafkaUtil;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * 1.读取配置表，表中配置了mysql的表，业务数据发到kafka中，维度数据发送到Hbase中，将此配置表封装并广播出去
 * 2.将配置表配置与mysqlBinlog数据流connect
 * 3.根据配置来分发数据，业务数据分发到kafka中，维度数据通过phoenix存储到hbase中去
 */


public class ProcessMysqlBinlog {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.setStateBackend(new FsStateBackend("hdfs://dataserver001:8020/gmall/dwd_log/ck"));
        env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000L);

        String topic = "ods_base_db";
        String groupId = "ods_db_group";

        // TODO 读取mysqlBinlog数据流
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaConsumer(topic, groupId);
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);

        //将数据转换成JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);

        //对数据进行过滤 过滤掉delete类型的数据
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                String type = value.getString("type");
                return !"delete".equals(type);
            }
        });

        // TODO 消费数据库中的配置表，将其处理成广播流

        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("dataserver001")
                .port(3306)
                .username("root")
                .password("800098XDc#@2021")
                .databaseList("gmall2021-realtime")
                .tableList("gmall2021-realtime.table_process")
                .deserializer(new DebeziumDeserializationSchema<String>() {
                    //反序列化方法
                    @Override
                    public void deserialize(SourceRecord sourceRecord, Collector<String> collector)
                            throws Exception {
                        //库名&表名
                        String topic = sourceRecord.topic();
                        String[] split = topic.split("\\.");
                        String db = split[1];
                        String table = split[2];
                        //获取数据
                        Struct value = (Struct) sourceRecord.value();
                        Struct after = value.getStruct("after");
                        JSONObject data = new JSONObject();
                        if (after != null) {
                            Schema schema = after.schema();
                            for (Field field : schema.fields()) {
                                data.put(field.name(), after.get(field.name()));
                            }
                        }
                        //获取操作类型
                        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
                        //创建 JSON 用于存放最终的结果
                        JSONObject result = new JSONObject();
                        result.put("database", db);
                        result.put("table", table);
                        result.put("type", operation.toString().toLowerCase());
                        result.put("data", data);
                        collector.collect(result.toJSONString());
                    }

                    //定义数据类型
                    @Override
                    public TypeInformation<String> getProducedType() {
                        return TypeInformation.of(String.class);
                    }
                })
                .build();

        DataStreamSource<String> tableProcessDS = env.addSource(sourceFunction);






    }
}
