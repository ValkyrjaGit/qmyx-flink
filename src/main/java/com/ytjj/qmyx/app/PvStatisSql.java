package com.ytjj.qmyx.app;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class PvStatisSql {
    public static void main(String[] args) throws Exception {
        // TODO 1.创建流环境和表环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // TODO 2.直接从kafka中创建动态表
        tableEnv.executeSql("create table KafkaTable(\n" +
                "platform string,channel string,\n" +
                "createTime bigint,\n" +
                "row_time as to_timestamp(from_unixtime(createTime/1000,'yyyy-MM-dd HH:mm:ss')),\n" +
                "watermark for row_time as row_time - interval '5' second\n" +
                ") \n" +
                "with ('connector'='kafka',\n" +
                "'topic'='qmyx_pv',\n" +
                "'properties.bootstrap.servers'='dataserver001:9092',\n" +
                "'properties.group.id'='test',\n" +
                "'scan.startup.mode'='latest-offset',\n" +
                "'format'='json'\n" +
                ")");

        //TODO  3.创建输出动态表
        tableEnv.executeSql("create table pv_temp (platform string,channel string,cnt int,sinktime timestamp) " +
                "with('connector'='jdbc','url'='jdbc:mysql://dataserver001:3306/test','table-name'='pv_temp','username'='root','password'='800098XDc#@2021')");

        //        Table table = tableEnv.sqlQuery("select * from KafkaTable");
        //        tableEnv.toAppendStream(table, Row.class).print();
        //        env.execute();

        //TODO 4.持续查询sql
        TableResult tableResult = tableEnv.executeSql("insert into pv_temp select platform,channel,cast(count(*) as int) as cnt,tumble_end(row_time,interval '5' second) as sinktime  from KafkaTable group by platform,channel,tumble(row_time,interval '5' second)");
        //        System.out.println(tableResult.getJobClient().get().getJobStatus());
        //        env.execute("test-job");
    }

}
