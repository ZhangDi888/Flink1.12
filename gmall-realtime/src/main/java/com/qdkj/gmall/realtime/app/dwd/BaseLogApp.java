package com.qdkj.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.qdkj.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 准备日志行为数据
 */
public class BaseLogApp {

    private static final String TOPIC_START = "dwd_start_log";
    private static final String TOPIC_DISPLAY = "dwd_display_log";
    private static final String TOPIC_PAGE = "dwd_page_log";

    public static void main(String[] args) throws Exception{

        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.1设置并行度，和kafka分区数保持一致
        env.setParallelism(4);

        //TODO 因为虚拟机开启hadoop占内存，所以检查点暂时不用，正常项目可以用
        //1.2设置检查点，保证消费精准一次，将读取数据的偏移量保存到检查点
        //设置5000毫秒保存一次检查点
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        //设置检查点的超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        //设置检查点保存的地方 8020是他的服务端口，不要写web端口
        env.setStateBackend(new FsStateBackend("hdfs://hadoop101:9820/gmall/checkpoint/baselogApp"));
        //linux的zhangdi用户没有对hdfs写得权限，通过代码添加，也可以直接在hdfs页面对他添加权限
//         System.setProperty("HADOOP_USER_NAME", "zhangdi"); xxxxxxxx

        //2.从kafka读取数据
        String topic = "ods_base_log";
        String groupid = "base_log_app_group";

        //执行消费kafka的topic以及消费者
        FlinkKafkaConsumer<String> kafakaSource = MyKafkaUtil.getKafakaSource(topic, groupid);
        DataStreamSource<String> kafkaDS = env.addSource(kafakaSource);

        //3.将kafka数据String类型转换Json
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(
                new MapFunction<String, JSONObject>() {

                    @Override
                    public JSONObject map(String s) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(s);
                        return jsonObject;
                    }
                }
        );

        /**
         * 对新老用户进行纠正，JSON里面有个is_new，新客户会写1，老客户0，有可能机器出故障重启之后，老客户也会显示新客户
         *使用键控状态，将mid作为key，他的日期保存到状态里，对key进行分组，如果新来的记录，相同的key，状态有日期，且和现在的日期不一样，说明是老客户，反之是新客户，如果是老客户，他的is_new就应该纠正为0
         */

        //对key进行分组
        KeyedStream<JSONObject, String> midkeyedDS = jsonObjDS.keyBy(
                data -> data.getJSONObject("common").getString("mid")
        );

        //状态有算子状态，还有键控状态，本次为了修复新老用户，使用键控状态合适
        SingleOutputStreamOperator<JSONObject> jsonDSWithFlag = midkeyedDS.map(
                new RichMapFunction<JSONObject, JSONObject>() {
                    //定义该mid的访问状态
                    private ValueState<String> firstVisitDataState;
                    //定义时间格式化对象
                    private SimpleDateFormat sdf;

                    @Override
                    public void open(Configuration parameters) throws Exception {

                        firstVisitDataState = getRuntimeContext().getState(
                                new ValueStateDescriptor<String>("newMidDataState", String.class)
                        );

                        sdf = new SimpleDateFormat("yyyyMMdd");
                    }

                    @Override
                    public JSONObject map(JSONObject jsonObject) throws Exception {
                        //获取当前一条数据标记状态
                        String isNew = jsonObject.getJSONObject("common").getString("is_new");

                        Long ts = jsonObject.getLong("ts");

                        if ("1".equals(isNew)) {
                            String stateDate = firstVisitDataState.value();
                            //取出当前数据的时间,并转格式
                            String curDate = sdf.format(new Date(ts));

                            if (stateDate != null && stateDate.length() != 0) {
                                if (!stateDate.equals(curDate)) {
                                    isNew = "0";
                                    jsonObject.getJSONObject("common").put("is_new", isNew);
                                }
                            } else {
                                //如果之前没保存过状态,现在就把当前时间保存为mid的状态
                                firstVisitDataState.update(curDate);
                            }

                        }
                        return jsonObject;
                    }
                }
        );

        /**
         * 数据分流
         * 1.创建侧输出流，根据曝光日志（包含display），启动日志（包含start），行为日志分流
         * 2.将上边的流调用process方法，根据关键字，判断流存到哪
         */

        //定义启动日志侧输出流
        OutputTag<String> startTag = new OutputTag<String>("start"){};
        //定义曝光日志侧输出流
        OutputTag<String> displayTag = new OutputTag<String>("display") {};

        SingleOutputStreamOperator<String> pageDS = jsonDSWithFlag.process(
                new ProcessFunction<JSONObject, String>() {
                    @Override
                    public void processElement(JSONObject jsonObject, Context context, Collector<String> out) throws Exception {
                        //先取出start相关数据，如果取出的值为null就else进入下一步判断
                        JSONObject start = jsonObject.getJSONObject("start");

                        //如果取出来的值不为null，就发送到start侧输出流
                        if (start != null && start.size() > 0) {

                            context.output(startTag, start.toString());

                        } else {

                            //这个是页面数据，其中包含了曝光数据
                            //如果不是曝光，启动数据，就是行为数据，将行为数据输出到主流
                            out.collect(jsonObject.toString());

                            JSONArray displays = jsonObject.getJSONArray("displays");

                            //因为display是数组，现在业务需要，拆开数组，一条条发送到侧输出流
                            if (displays != null && displays.size() > 0) {

                                for (int i = 0; i < displays.size(); i++) {
                                    JSONObject jsonObject1 = displays.getJSONObject(i);
                                    //如果想添加页面id，先取出来，然后加进去
                                    String page_id = jsonObject.getJSONObject("page").getString("page_id");
                                    jsonObject1.put("page_id", page_id);
                                    context.output(displayTag, jsonObject1.toString());
                                }
                            }
                        }
                    }
                }
        );

        //获取启动侧输出流
        DataStream<String> startDS = pageDS.getSideOutput(startTag);
        //获取曝光侧输出流
        DataStream<String> disDS = pageDS.getSideOutput(displayTag);
        //获取主输出流
//        pageDS.print("first");

        //将start日志发送到topic dwd_start_log
        FlinkKafkaProducer<String> startSink = MyKafkaUtil.getKafkaSink(TOPIC_START);
        startDS.addSink(startSink);

        FlinkKafkaProducer<String> displaySink = MyKafkaUtil.getKafkaSink(TOPIC_DISPLAY);
        disDS.addSink(displaySink);

        FlinkKafkaProducer<String> pageSink = MyKafkaUtil.getKafkaSink(TOPIC_PAGE);
        pageDS.addSink(pageSink);

        disDS.print("dis");

        env.execute();
    }
}
