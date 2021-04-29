package com.qdkj.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.qdkj.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 统计UV
 */
public class UniqueVisitApp {
    public static void main(String[] args) throws Exception {
        //1.0创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.0创建本地环境,可以在本地看flink界面,方便调试
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        //1.1设置并行度
        env.setParallelism(4);

        //1.2设置检查点
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        //1.3设置检查点超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        //1.4设置状态保存地方
        env.setStateBackend(new FsStateBackend("hdfs://hadoop101:9820/gmall/checkpoint/uniquevisitapp"));

        //2.0从kafka消费数据
        String sourceTopic = "dwd_page_log";
        String groupid = "unique_visit_app_group";
        String sinkTopic = "dwm_unique_visit";

        FlinkKafkaConsumer<String> kafakaSource = MyKafkaUtil.getKafakaSource(sourceTopic, groupid);
        //读取kafka数据
        DataStreamSource<String> jsonStrDS = env.addSource(kafakaSource);

        //2.1处理业务
        //将String转换成json对象
        SingleOutputStreamOperator<JSONObject> jsonMapDs = jsonStrDS.map(
                jsonDS -> JSON.parseObject(jsonDS)
        );
        //根据设备id分组
        KeyedStream<JSONObject, String> jsonObjByKeyDS = jsonMapDs.keyBy(jsonObject -> jsonObject.getJSONObject("common").getString("mid"));

        //过滤
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjByKeyDS.filter(
                new RichFilterFunction<JSONObject>() {
                    //声明时间样式以及状态
                    SimpleDateFormat sdf = null;
                    ValueState<String> lastsVisitDateState = null;

                    /**
                     * 初始化时间格式,以及状态
                     * @param parameters
                     * @throws Exception
                     */
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        sdf = new SimpleDateFormat("yyyyMMdd");
                        //初始化状态,状态名称lastsVisitDateState,数据类型String
                        ValueStateDescriptor<String> lastsVisitDateStateDes = new ValueStateDescriptor<>("lastsVisitDateState", String.class);
                        //因为我们做的是日活,所以状态数据只在当天有效,过一天就可以失效掉
                        StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(Time.days(1)).build();
                        lastsVisitDateStateDes.enableTimeToLive(stateTtlConfig);

                        this.lastsVisitDateState = getRuntimeContext().getState(lastsVisitDateStateDes);

                    }

                    @Override
                    public boolean filter(JSONObject jsonObject) throws Exception {
                        String pageId = jsonObject.getJSONObject("page").getString("last_page_id");

                        //如果last_page_id有值，说明不是第一次进来，要过滤掉
                        if (pageId != null && pageId.length() > 0) {
                            return false;
                        }

                        //取出json的时间戳，和状态的时间戳做对比，如果时间戳和状态的一样要过滤掉，如果不一样就保留同时更新状态
                        Long ts = jsonObject.getLong("ts");
                        String dataTime = sdf.format(new Date(ts));
                        String dataState = lastsVisitDateState.value();

                        if (dataState != null && dataState.length() > 0 && dataState.equals(dataTime)) {
                            System.out.println("当前的" + dataState + "已存在，过滤掉");
                            return false;
                            //else说明当前状态没有值，或者传过来的数据的时间戳和状态不一样，，保留，同时更新状态
                        } else {
                            lastsVisitDateState.update(dataTime);
                            return true;
                        }
                    }
                }
        );

        //2.2保存到kafka, 先将Json数据转换成String，之前将String转换成json是方便操作，现在需要写到kafka
        SingleOutputStreamOperator<String> stringFilterDs = filterDS.map(jsonObject -> jsonObject.toJSONString());
        stringFilterDs.addSink(MyKafkaUtil.getKafkaSink("dwm_unique_visit"));
        //3.0执行
        env.execute();

    }
}
