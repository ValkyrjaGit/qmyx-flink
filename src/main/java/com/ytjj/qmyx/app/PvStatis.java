package com.ytjj.qmyx.app;

import com.alibaba.fastjson.JSON;
import com.ytjj.qmyx.beans.Pv;
import com.ytjj.qmyx.beans.PvSink;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.util.Properties;

import static org.apache.flink.table.api.Expressions.$;

public class PvStatis {
    public static void main(String[] args) throws Exception {
        // TODO  创建流环境及表环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 1.用dataStream方式从kafka读取数据
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "dataserver001:9092");
        // 下面这些次要参数
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        // flink添加外部数据源
        DataStream<String> dataStream = env.addSource(new FlinkKafkaConsumer<String>("qmyx_pv", new SimpleStringSchema(), properties));

        //2.将dataStream转换为Pv的Pojo类型
        DataStream<Pv> pvDataStream = dataStream.map(line -> {
            Pv pv = JSON.parseObject(line, Pv.class);
            return pv;
        });

        //TODO 03.为数据流添加水位线
        SingleOutputStreamOperator<Pv> pvStreamWithWM = pvDataStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Pv>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Pv>() {
                            @Override
                            public long extractTimestamp(Pv pv, long l) {
                                return pv.getBrowsetime();
                            }
                        })
        );

        // TODO O4.将流转化成表,并指定时间属性列
        Table table = tableEnv.fromDataStream(pvStreamWithWM, $("platform"), $("channel"), $("createtime").rowtime());
        tableEnv.createTemporaryView("pv", table);

        // TODO 05.FlinkSql
        Table result = tableEnv.sqlQuery(" select platform,channel,cast(count(*) as int) as cnt,tumble_end(createtime,interval '5' second) as sinktime  from pv group by platform,channel,tumble(createtime,interval '5' second) ");

        DataStream<PvSink> rowDataStream = tableEnv.toAppendStream(result, PvSink.class);


        //TODO 06.将结果sink到Mysql中
        rowDataStream.addSink(new MyJdbcSink());

//        rowDataStream.print();
        env.execute();
    }

    public static class MyJdbcSink extends RichSinkFunction<PvSink> {
        // 声明连接和预编译语句
        Connection connection = null;
        PreparedStatement insertStmt = null;
        PreparedStatement updateStmt = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection("jdbc:mysql://dataserver001:3306/test?useUnicode=true&serverTimezone=Asia/Shanghai&characterEncoding=UTF-8&useSSL=false", "root", "800098XDc#@2021");
            insertStmt = connection.prepareStatement("insert into pv_temp (platform, channel,cnt,sinktime) values (?, ?,?,?)");
//            updateStmt = connection.prepareStatement("update pv_temp set temp = ? where id = ?");
        }

        @Override
        public void invoke(PvSink value, Context context) throws Exception {
            insertStmt.setString(1, value.getPlatform());
            insertStmt.setString(2, value.getChannel());
            insertStmt.setInt(3, value.getCnt());
            insertStmt.setTimestamp(4, value.getSinktime());
            insertStmt.execute();
        }

        @Override
        public void close() throws Exception {
            insertStmt.close();
            updateStmt.close();
            connection.close();
        }
    }

}
