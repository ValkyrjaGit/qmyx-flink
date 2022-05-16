package com.ytjj.qmyx.bean;

import lombok.Data;

@Data
public class TableProcess {
    //动态分流 Sink 常量
    public static final String SINK_TYPE_HBASE = "hbase";
    public static final String SINK_TYPE_KAFKA = "kafka";
    public static final String SINK_TYPE_CK = "clickhouse";

    //来源表(表名)
    String sourceTable;
    //操作类型 insert,update,delete（新增和变化的数据可能要放到不同的地方）
    String operateType;
    //输出类型 hbase kafka
    String sinkType;
    //输出表(主题)（输出到hbase或者kafka中的表名）
    String sinkTable;
    //输出字段(为了能让phoenix自动建表)
    String sinkColumns;
    //主键字段（phoenix建表必须要指定主键）
    String sinkPk;
    //建表扩展
    String sinkExtend;
}
