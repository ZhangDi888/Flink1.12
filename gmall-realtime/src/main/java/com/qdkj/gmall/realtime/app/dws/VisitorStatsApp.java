package com.qdkj.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ibm.icu.text.SimpleDateFormat;
import com.qdkj.gmall.realtime.bean.VisitorStats;
import com.qdkj.gmall.realtime.utils.ClickhouseUtil;
import com.qdkj.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Date;

/**
 * Author: Felix
 * Date: 2021/2/22
 * Desc:  访客主题统计
 * 需要启动的服务
 * -logger.sh(Nginx以及日志处理服务)、zk、kafka
 * -BaseLogApp、UniqueVisitApp、UserJumpDetailApp、VisitorStatsApp
 * 执行流程分析
 * -模拟生成日志数据
 * -交给Nginx进行反向代理
 * -交给日志处理服务 将日志发送到kafka的ods_base_log
 * -BaseLogApp从ods层读取数据，进行分流，将分流的数据发送到kakfa的dwd(dwd_page_log)
 * -UniqueVisitApp从dwd_page_log读取数据，将独立访客明细发送到dwm_unique_visit
 * -UserJumpDetailApp从dwd_page_log读取数据，将页面跳出明细发送到dwm_user_jump_detail
 * -VisitorStatsApp
 * >从dwd_page_log读取数据，计算pv、持续访问时间、session_count
 * >从dwm_unique_visit读取数据，计算uv
 * >从dwm_user_jump_detail读取数据，计算页面跳出
 * >输出
 * >统一格式 合并
 * >分组、开窗、聚合
 * >将聚合统计的结果保存到ClickHouse OLAP数据库
 */
public class VisitorStatsApp {
    public static void main(String[] args) throws Exception {
        //TODO 0.基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(4);

        //检查点CK相关设置
        env.enableCheckpointing(5000, CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        StateBackend fsStateBackend = new FsStateBackend(
                "hdfs://hadoop101:9820/gmall/flink/checkpoint/VisitorStatsApp");
        env.setStateBackend(fsStateBackend);
        //设置增加操作hdfs的用户名权限
        System.setProperty("HADOOP_USER_NAME","zhangdi");

        String groupId = "visitor_stats_app";

        //TODO 1.从Kafka的pv、uv、跳转明细主题中获取数据
        String pageViewSourceTopic = "dwd_page_log";
        String uniqueVisitSourceTopic = "dwm_unique_visit";
        String userJumpDetailSourceTopic = "dwm_user_jump_detail";

        FlinkKafkaConsumer<String> pageViewSource = MyKafkaUtil.getKafakaSource(pageViewSourceTopic, groupId);
        FlinkKafkaConsumer<String> uniqueVisitSource = MyKafkaUtil.getKafakaSource(uniqueVisitSourceTopic, groupId);
        FlinkKafkaConsumer<String> userJumpSource = MyKafkaUtil.getKafakaSource(userJumpDetailSourceTopic, groupId);

        DataStreamSource<String> pageViewDStream = env.addSource(pageViewSource);
        DataStreamSource<String> uniqueVisitDStream = env.addSource(uniqueVisitSource);
        DataStreamSource<String> userJumpDStream = env.addSource(userJumpSource);

        //对各个流进行格式转换 jsonStr -> VisitorStats
        //pv流转换
        SingleOutputStreamOperator<VisitorStats> pvStatsDS = pageViewDStream.map(
                new MapFunction<String, VisitorStats>() {
                    @Override
                    public VisitorStats map(String page) throws Exception {
                        //先将String转成Json,这样的话方便对数据进行操作
                        JSONObject jsonObject = JSON.parseObject(page);
                        VisitorStats visitorStats = new VisitorStats(
                                "",
                                "",
                                jsonObject.getJSONObject("common").getString("vc"),
                                jsonObject.getJSONObject("common").getString("ch"),
                                jsonObject.getJSONObject("common").getString("ar"),
                                jsonObject.getJSONObject("common").getString("is_new"),
                                0L,
                                1L,
                                0L,
                                0L,
                                jsonObject.getJSONObject("page").getLong("during_time"),
                                jsonObject.getLong("ts")
                        );

                        return visitorStats;
                    }
                }
        );

        //转换uv流
        SingleOutputStreamOperator<VisitorStats> uniqueVisitDS = uniqueVisitDStream.map(
                new MapFunction<String, VisitorStats>() {
                    @Override
                    public VisitorStats map(String s) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(s);
                        VisitorStats visitorStats = new VisitorStats(
                                "",
                                "",
                                jsonObject.getJSONObject("common").getString("vc"),
                                jsonObject.getJSONObject("common").getString("ch"),
                                jsonObject.getJSONObject("common").getString("ar"),
                                jsonObject.getJSONObject("common").getString("is_new"),
                                0L,
                                0L,
                                1L,
                                0L,
                                0L,
                                jsonObject.getLong("ts")
                        );
                        return visitorStats;
                    }
                }
        );

        //转换sv流, 需要先过滤出last_page_id为空的值，然后在映射成对象
        SingleOutputStreamOperator<VisitorStats> sessionDS = pageViewDStream.process(
                new ProcessFunction<String, VisitorStats>() {
                    @Override
                    public void processElement(String jsonStr, Context context, Collector<VisitorStats> collector) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(jsonStr);
                        String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");

                        if (lastPageId == null && lastPageId.length() == 0) {
                            VisitorStats visitorStats = new VisitorStats(
                                    "",
                                    "",
                                    jsonObject.getJSONObject("common").getString("vc"),
                                    jsonObject.getJSONObject("common").getString("ch"),
                                    jsonObject.getJSONObject("common").getString("ar"),
                                    jsonObject.getJSONObject("common").getString("is_new"),
                                    0L,
                                    0L,
                                    1L,
                                    0L,
                                    0L,
                                    jsonObject.getLong("ts")
                            );
                            collector.collect(visitorStats);
                        }
                    }
                }
        );

        //userJumpDStream转换
        SingleOutputStreamOperator<VisitorStats> uvDS = userJumpDStream.map(
                new MapFunction<String, VisitorStats>() {
                    @Override
                    public VisitorStats map(String jsonStr) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(jsonStr);
                        VisitorStats visitorStats = new VisitorStats(
                                "",
                                "",
                                jsonObject.getJSONObject("common").getString("vc"),
                                jsonObject.getJSONObject("common").getString("ch"),
                                jsonObject.getJSONObject("common").getString("ar"),
                                jsonObject.getJSONObject("common").getString("is_new"),
                                0L,
                                0L,
                                0L,
                                1L,
                                0L,
                                jsonObject.getLong("ts")
                        );
                        return visitorStats;
                    }
                }
        );

        //用union将几条流的数据进行合并
        DataStream<VisitorStats> unionDS = pvStatsDS.union(uniqueVisitDS, sessionDS, uvDS);

        //设定waterMark 以及提取事件时间字段
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithWaterMarkDS = unionDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<VisitorStats>() {
                            @Override
                            public long extractTimestamp(VisitorStats visitorStats, long l) {
                                return visitorStats.getTs();
                            }
                        })
        );

        //分组, 按照地区，渠道，版本，新老访客维度进行分组，因为这里是四个维度，所以将他们封装一个Tuple4
        KeyedStream<VisitorStats, Tuple4<String, String, String, String>> visitorStatsTuple4KeyedDS = visitorStatsWithWaterMarkDS.keyBy(
                //Tuple4导的是flink的包，不是scala
                new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
                    @Override
                    public Tuple4<String, String, String, String> getKey(VisitorStats visitorStats) throws Exception {
                        return Tuple4.of(
                                visitorStats.getAr(),
                                visitorStats.getCh(),
                                visitorStats.getVc(),
                                visitorStats.getIs_new()
                        );
                    }
                }
        );

        //对流进行一个10S的开窗
        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> windowDS = visitorStatsTuple4KeyedDS.window(TumblingEventTimeWindows.of(Time.seconds(10)));


        //对数据进行聚合，并提炼出窗口的开始时间结束时间
        SingleOutputStreamOperator<VisitorStats> reduceDS = windowDS.reduce(
                new ReduceFunction<VisitorStats>() {
                    //对度量值进行聚合
                    @Override
                    public VisitorStats reduce(VisitorStats visitorStats1, VisitorStats visitorStats2) throws Exception {
                        visitorStats1.setPv_ct(visitorStats1.getPv_ct() + visitorStats2.getPv_ct());
                        visitorStats1.setSv_ct(visitorStats1.getSv_ct() + visitorStats2.getSv_ct());
                        visitorStats1.setUv_ct(visitorStats1.getUv_ct() + visitorStats2.getUv_ct());
                        visitorStats1.setDur_sum(visitorStats1.getDur_sum() + visitorStats2.getDur_sum());
                        return visitorStats1;
                    }
                },
                //获取窗口的开始时间结束时间
                new ProcessWindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
                    @Override
                    public void process(Tuple4<String, String, String, String> stringStringStringStringTuple4, Context context, Iterable<VisitorStats> iterable, Collector<VisitorStats> collector) throws Exception {
                        //因为context.window().getStart()获取的是时间戳，需要格式化
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        //对窗口的数据进行遍历，获取窗口开始时间以及结束时间，填充到当前数据里面
                        for (VisitorStats visitorStats : iterable) {
                            String startTime = sdf.format(new Date(context.window().getStart()));
                            String endTime = sdf.format(new Date(context.window().getEnd()));
                            visitorStats.setStt(startTime);
                            visitorStats.setEdt(endTime);
                            visitorStats.setTs(new Date().getTime());
                            //将结果传递下游
                            collector.collect(visitorStats);
                        }
                    }
                }
        );
        reduceDS.print();
        reduceDS.addSink(ClickhouseUtil.getJdbcSink("insert into visitor_stats_2021 values(?,?,?,?,?,?,?,?,?,?,?,?)"));

        env.execute();
    }
}
