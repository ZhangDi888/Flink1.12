package com.qdkj.gmall.realtime.app.dws;

import com.qdkj.gmall.realtime.bean.KeywordStats;
import com.qdkj.gmall.realtime.common.GmallConstant;
import com.qdkj.gmall.realtime.func.KeywordUDTF;
import com.qdkj.gmall.realtime.utils.ClickhouseUtil;
import com.qdkj.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class KeywordStatsApp {
    public static void main(String[] args) throws Exception {
        //TODO: 1.创建flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(4);
        //设置检查点
        env.enableCheckpointing(5000, CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        StateBackend fsStateBackend = new FsStateBackend(
                "hdfs://hadoop202:8020/gmall/flink/checkpoint/ProductStatsApp");
        env.setStateBackend(fsStateBackend);
        System.setProperty("HADOOP_USER_NAME","zhangdi");

        //创建表环境
        EnvironmentSettings setting = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, setting);

        //TODO: 2.注册UDTF函数
        tableEnv.createTemporaryFunction("ik_analyze", KeywordUDTF.class);
        //TODO: 3.创建动态表
        //声明消费者以及消费者组
        String topic = "dwd_page_log";
        String groupId = "Keywordstats_app_group";
        //FROM_UNIXTIME: 将距离1970-01-01 00:00:00的秒，转换为指定格式字符串
        //TO_TIMESTAMP: 将字符串日期转为timestamp
        tableEnv.executeSql(
            "CREATE TABLE page_view (" +
                    "common MAP(STRING, STRING)," +
                    "page MAP(STRING, STRING)," +
                    "ts BIGINT," +
                    "rowtime as TO_TIMESTAMP(FROM_UNIXTIME(ts/1000, 'yyyy-MM-dd HH:mm:ss'))," +
                    "WATERMARK FOR rowtime AS rowtime - INTERVAL '2' SECOND)" +
                    "WITH (" + MyKafkaUtil.getKafkaDDL(topic, groupId) + ")" +
                    ");"
       );

        //TODO: 4.从动态表查数据
        Table table = tableEnv.sqlQuery("select page['item'] fullword, rowtime" +
                " from page_view where page['page_id'] = 'good_list' and page['item'] is not null");

        //TODO: 5.利用自定义函数对关键词进行拆分
        Table keywordTable = tableEnv.sqlQuery("SELECT keyword, rowtime" +
                " FROM" + table + "," +
                "LATERAL TABLE(ik_analyze(fullword)) AS t(keyword)");

        //TODO: 6.聚合
        Table reduceTable = tableEnv.sqlQuery(
                "select keyword, count(*), '" + GmallConstant.KEYWORD_SEARCH + "' source," +
                        "DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND), 'yyyy-MM-dd HH:mm:ss') stt," +
                        "DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND), 'yyyy-MM-dd HH:mm:ss') edt," +
                        "UNIX_TIMESTAMP()*1000 ts from " + keywordTable + " group by TUMBLE(rowtime, INTERVAL '10' SECOND),keyword"
        );

        //TODO: 7.转换为流
        DataStream<KeywordStats> keywordStatsDS = tableEnv.toAppendStream(reduceTable, KeywordStats.class);
        keywordStatsDS.print(">>>>>");

        //TODO: 8.ClickHouse
        keywordStatsDS.addSink(ClickhouseUtil.getJdbcSink("insert into keyword_stats_2021(keyword,ct,source,stt,edt,ts) values(?,?,?,?,?,?)"));
        env.execute();
    }
}
