package com.qdkj.gmall.realtime.app.dws;

import com.qdkj.gmall.realtime.bean.ProvinceStats;
import com.qdkj.gmall.realtime.utils.ClickhouseUtil;
import com.qdkj.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ProvinceStatsSqlApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        //1.1 创建Flink流式处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);
        /*
        //1.3 检查点CK相关设置
        env.enableCheckpointing(5000, CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        StateBackend fsStateBackend = new FsStateBackend(
                "hdfs://hadoop202:8020/gmall/flink/checkpoint/ProductStatsApp");
        env.setStateBackend(fsStateBackend);
        System.setProperty("HADOOP_USER_NAME","atguigu");
        */
        //创建表环境
        EnvironmentSettings setting = EnvironmentSettings
                .newInstance()
                //这个是选择流模式还是批模式，默认流模式
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, setting);

        //TODO 2.把数据源定义为动态表
        String groupId = "province_stats";
        String orderWideTopic = "dwm_order_wide";

        //对kafka的数据转为动态流
        tableEnv.executeSql(
                "CREATE TABLE ORDER_WIDE (province_id BIGINT, " +
                        "province_name STRING,province_area_code STRING" +
                        ",province_iso_code STRING,province_3166_2_code STRING,order_id STRING, " +
                        "split_total_amount DOUBLE,create_time STRING,rowtime AS TO_TIMESTAMP(create_time) ," +
                        "WATERMARK FOR  rowtime  AS rowtime)" +
                        " WITH (" + MyKafkaUtil.getKafkaDDL(orderWideTopic, groupId) + ")"
        );

        //TODO 3.聚合计算
        Table provinceStateTable = tableEnv.sqlQuery(
                "select " +
                        //设置表的窗口大小，TUMBLE_START开始时间TUMBLE_END结束时间，指定rowtime为窗口的时间字段，INTERVAL '10' SECOND窗口的大小
                        "DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND ),'yyyy-MM-dd HH:mm:ss') stt, " +
                        "DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND ),'yyyy-MM-dd HH:mm:ss') edt , " +
                        //查询相关维度
                        " province_id,province_name,province_area_code area_code," +
                        "province_iso_code iso_code ,province_3166_2_code iso_3166_2 ," +
                        "COUNT( DISTINCT  order_id) order_count, sum(split_total_amount) order_amount," +
                        //当前的系统时间*1000转为毫秒数
                        "UNIX_TIMESTAMP()*1000 ts " +
                        //group by 开窗
                        " from  ORDER_WIDE group by  TUMBLE(rowtime, INTERVAL '10' SECOND )," +
                        //根据维度开窗
                        " province_id,province_name,province_area_code,province_iso_code,province_3166_2_code ");

        //TODO 4.将动态表转为流,只涉及insert用toAppendStream，涉及变化用toRetractStream
        DataStream<ProvinceStats> provinceStatsDataStream = tableEnv.toAppendStream(provinceStateTable, ProvinceStats.class);
        provinceStatsDataStream.print("provinceStatsDataStream>>>>>>>>>");
        //TODO 5.写入ck
        provinceStatsDataStream.addSink(ClickhouseUtil.getJdbcSink("insert into province_stats_2021  values(?,?,?,?,?,?,?,?,?,?)"));

        env.execute();
    }
}
