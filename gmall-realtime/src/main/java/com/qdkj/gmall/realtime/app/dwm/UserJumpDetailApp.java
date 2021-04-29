package com.qdkj.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.inject.internal.util.$ObjectArrays;
import com.qdkj.gmall.realtime.utils.MyKafkaUtil;
import org.apache.commons.codec.binary.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * 过滤出用户跳出明细
 */
public class UserJumpDetailApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        //开启检查点
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop101:9820/gmall/checkpoint/uniquevisitapp"));

        String groupid = "user_jump_detail_group";
        String topic = "dwd_page_log";
        String sinkTopic = "user_jump_detail_app";
        FlinkKafkaConsumer<String> kafakaSource = MyKafkaUtil.getKafakaSource(topic, groupid);

//        DataStream<String> dataStream = env
//                .fromElements(
//                        "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":10000} ",
//                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"home\"},\"ts\":12000}",
//                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
//                                "\"home\"},\"ts\":35000} ",
//                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
//                                "\"detail\"},\"ts\":30000} "
//                );


        DataStreamSource<String> stringDS = env.addSource(kafakaSource);
        //将数据转成JSONObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = stringDS.map(date -> JSON.parseObject(date));

        //指定事件时间字段
        SingleOutputStreamOperator<JSONObject> jsonObjWithTSDS = jsonObjDS.assignTimestampsAndWatermarks(
                //设置水位线，因为没有迟到现象，“forMonotonousTimestamps”根据时间单调递增，"withTimestampAssigner"指定哪个字段为时间戳
                WatermarkStrategy.<JSONObject>forMonotonousTimestamps().withTimestampAssigner(
                        new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject jsonObject, long l) {
                                return jsonObject.getLong("ts");
                            }
                        })

        );

        //将数据流根据mid进行分组
        KeyedStream<JSONObject, String> keyBymidDS = jsonObjWithTSDS.keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid"));
        //配置CEP规则
        Pattern<JSONObject, JSONObject> withinPattern = Pattern.<JSONObject>begin("first")
                .where(//获取页面没有last_page_id的数据，说明他是第一次访问
                        new SimpleCondition<JSONObject>() {
                            @Override
                            public boolean filter(JSONObject jsonObject) throws Exception {
                                String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                                //如果lastPageId没有值，就保留
                                if (lastPageId == null || lastPageId.length() == 0) {
                                    return true;
                                }
                                return false;
                            }
                        }
                ).next("next")
                .where(//判断10秒内是否有访问其他页面，这只是个条件，条件是访问了其他页面，配合上边没有last_page_id，在10秒内，组成的cep
                        new SimpleCondition<JSONObject>() {
                            @Override
                            public boolean filter(JSONObject jsonObject) throws Exception {
                                String pageId = jsonObject.getJSONObject("page").getString("page_id");
                                if (pageId != null && pageId.length() > 0) {
                                    return true;
                                }
                                return false;
                            }
                        }
                )
                //判断条件，10秒内
                .within(Time.milliseconds(10000));

        //根据CEP规则表达式筛选
        PatternStream<JSONObject> cepPatternDS = CEP.pattern(keyBymidDS, withinPattern);

        //将超时的数据输出到侧输出流
        OutputTag<String> timeOutTag = new OutputTag<String>("timeOut"){};

        SingleOutputStreamOperator<Object> filterDS = cepPatternDS.flatSelect(
                timeOutTag,
                //这个是处理超时的数据
                new PatternFlatTimeoutFunction<JSONObject, String>() {
                    @Override
                    public void timeout(Map<String, List<JSONObject>> pattern, long l, Collector<String> collector) throws Exception {
                        //因为CEP规则是超时数据,只需要取出first规则的数据,就可以得到
                        List<JSONObject> firstObj = pattern.get("first");
                        for (JSONObject jsonObject : firstObj) {
                            collector.collect(jsonObject.toJSONString());
                        }
                    }
                },
                //处理没有超时的数据
                new PatternFlatSelectFunction<JSONObject, Object>() {
                    @Override
                    public void flatSelect(Map<String, List<JSONObject>> map, Collector<Object> collector) throws Exception {
                        //业务没需求,所以这块不需要管他
                    }
                }
        );

        //从侧输出流获取超时数据
        DataStream<String> sideOutputDS = filterDS.getSideOutput(timeOutTag);

        sideOutputDS.addSink(MyKafkaUtil.getKafkaSink(sinkTopic));

        env.execute();
    }
}
