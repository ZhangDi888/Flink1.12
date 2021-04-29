package com.qdkj.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.qdkj.gmall.realtime.bean.OrderDetail;
import com.qdkj.gmall.realtime.bean.OrderInfo;
import com.qdkj.gmall.realtime.bean.OrderWide;
import com.qdkj.gmall.realtime.func.DimAsyncFunction;
import com.qdkj.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class OrderWideApp {
    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop101:9820/gmall/checkpoint/orderwideapp"));


        //定义消费的topic以及消费者组，以及处理完发往哪个topic
        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String orderWideSinkTopic = "dwm_order_wide";
        String groupId = "order_wide_group";

        //消费订单数据
        FlinkKafkaConsumer<String> orderInfoSource = MyKafkaUtil.getKafakaSource(orderInfoSourceTopic, groupId);
        DataStreamSource<String> orderInfoDS = env.addSource(orderInfoSource);

        //消费订单详情数据
        FlinkKafkaConsumer<String> orderDetailSource = MyKafkaUtil.getKafakaSource(orderDetailSourceTopic, groupId);
        DataStreamSource<String> orderDetailDS = env.addSource(orderDetailSource);

        //对订单数据进行结构转换，转成bean
        SingleOutputStreamOperator<OrderInfo> orderInfoBeanDS = orderInfoDS.map(
                new RichMapFunction<String, OrderInfo>() {
                    //初始化时间格式
                    SimpleDateFormat sdf = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    }

                    @Override
                    public OrderInfo map(String s) throws Exception {
                        //用JSON工具，将String类型的数据转成bean对象，方便后续操作
                        OrderInfo orderInfo = JSON.parseObject(s, OrderInfo.class);
                        //将时间字段通过时间格式化，先把String转成时间格式，然后在转成时间戳
                        long timeStamp = sdf.parse(orderInfo.getCreate_time()).getTime();
                        orderInfo.setCreate_ts(timeStamp);
                        return orderInfo;
                    }
                }
        );

        SingleOutputStreamOperator<OrderDetail> orderDetailBeanDS = (SingleOutputStreamOperator<OrderDetail>) orderDetailDS.map(
                new RichMapFunction<String, OrderDetail>() {
                    //初始化时间格式
                    SimpleDateFormat sdf = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    }

                    @Override
                    public OrderDetail map(String s) throws Exception {
                        //用JSON工具，将String类型的数据转成bean对象，方便后续操作
                        OrderDetail orderDetail = JSON.parseObject(s, OrderDetail.class);
                        //将时间字段通过时间格式化，先把String转成时间格式，然后在转成时间戳
                        long time = sdf.parse(orderDetail.getCreate_time()).getTime();
                        orderDetail.setCreate_ts(time);
                        return orderDetail;
                    }
                }
        );
       //为订单指定时间戳,因为后面要双流jion,到时候有时间范围,需要用到时间戳
        SingleOutputStreamOperator<OrderInfo> orderInfoWithTsDS = orderInfoBeanDS.assignTimestampsAndWatermarks(
                //允许数据迟到3秒
                WatermarkStrategy.<OrderInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<OrderInfo>() {
                                    @Override
                                    public long extractTimestamp(OrderInfo orderInfo, long l) {
                                        return orderInfo.getCreate_ts();
                                    }
                                }
                        )
        );

        //为订单详情指定时间戳
        SingleOutputStreamOperator<OrderDetail> orderDetailWithTsDS = orderDetailBeanDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<OrderDetail>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
                            @Override
                            public long extractTimestamp(OrderDetail orderDetail, long l) {
                                return orderDetail.getCreate_ts();
                            }
                        })
        );

        //双流join,需要先把两个流按照关联字段进行分组
        KeyedStream<OrderInfo, Long> orderInfoBKeyDS = orderInfoWithTsDS.keyBy(OrderInfo::getId);
        KeyedStream<OrderDetail, Long> orderDetailBkeyDS = orderDetailWithTsDS.keyBy(OrderDetail::getOrder_id);

        //双流join
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderInfoBKeyDS
                //范围join
                .intervalJoin(orderDetailBkeyDS)
                //时间范围,比如现在时间是10,范围就是5-15时间数据
                .between(Time.milliseconds(-5), Time.milliseconds(5))
                .process(
                        new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                            @Override
                            public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, Context context, Collector<OrderWide> collector) throws Exception {
                                collector.collect(new OrderWide(orderInfo, orderDetail));
                            }
                        }
                );

        SingleOutputStreamOperator<OrderWide> orderWideWithUserDS = AsyncDataStream.unorderedWait(orderWideDS, new DimAsyncFunction<OrderWide>("DIM_USER_INFO") {
            @Override
            public String getKey(OrderWide obj) {
                return obj.getUser_id().toString();
            }

            //子类不允许比父类抛出的异常等级高，所以需要在join的抽象方法中抛出异常
            @Override
            public void join(OrderWide obj, JSONObject dimInfoJsonObj) throws Exception {
                //获取维度表的生日
                String birthday = dimInfoJsonObj.getString("BIRTHDAY");
                //对生日进行格式化
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                Date birthdayDate = sdf.parse(birthday);
                //获取生日的毫秒数
                Long birthdayTs = birthdayDate.getTime();
                //获取当前时间的毫秒数
                Long curTs = System.currentTimeMillis();
                //年龄毫秒数
                Long ageTs = curTs - birthdayTs;
                //年龄
                Long ageLong = ageTs / 1000L / 60L / 60L / 24L / 365L;
                int age = ageLong.intValue();

                obj.setUser_age(age);

                obj.setUser_gender(dimInfoJsonObj.getString("GENDER"));

            }
        }, 60, TimeUnit.SECONDS);

        //用异步查询对省份做一个关联
        SingleOutputStreamOperator<OrderWide> orderWideWithProDS = AsyncDataStream.unorderedWait(
                orderWideWithUserDS, new DimAsyncFunction<OrderWide>("DIM_BASE_PROVINCE") {
                    @Override
                    public String getKey(OrderWide obj) {
                        return obj.getProvince_id().toString();
                    }

                    @Override
                    public void join(OrderWide obj, JSONObject dimInfoJsonObj) throws Exception {
                        obj.setProvince_3166_2_code(dimInfoJsonObj.getString("ISO_3166_2"));
                        obj.setProvince_name(dimInfoJsonObj.getString("NAME"));
                        obj.setProvince_iso_code(dimInfoJsonObj.getString("ISO_CODE"));
                        obj.setProvince_area_code(dimInfoJsonObj.getString("AREA_CODE"));
                    }
                }
                //60是超时时间，TimeUnit.SECONDS为时间单位
                , 60, TimeUnit.SECONDS);

        //对SKU进行维度关联
        SingleOutputStreamOperator<OrderWide> orderWideSkuDS = AsyncDataStream.unorderedWait(orderWideWithProDS, new DimAsyncFunction<OrderWide>("DIM_SKU_INFO") {
            @Override
            public String getKey(OrderWide obj) {
                return obj.getSku_id().toString();
            }

            @Override
            public void join(OrderWide obj, JSONObject dimInfoJsonObj) throws Exception {
                obj.setSpu_id(dimInfoJsonObj.getLong("SPU_ID"));
                obj.setCategory3_id(dimInfoJsonObj.getLong("CATEGORY3_ID"));
                obj.setTm_id(dimInfoJsonObj.getLong("TM_ID"));
            }
        }, 60, TimeUnit.SECONDS);

        //对SPU商品进行维度关联
        SingleOutputStreamOperator<OrderWide> orderWideSpu = AsyncDataStream.unorderedWait(orderWideSkuDS, new DimAsyncFunction<OrderWide>("DIM_SPU_INFO") {

            @Override
            public String getKey(OrderWide obj) {
                return obj.getSpu_id().toString();
            }

            @Override
            public void join(OrderWide obj, JSONObject dimInfoJsonObj) throws Exception {
                obj.setSpu_name(dimInfoJsonObj.getString("SPU_NAME"));
            }
        }, 60, TimeUnit.SECONDS);

        //对品牌维度进行关联
        SingleOutputStreamOperator<OrderWide> orderWideTmDS = AsyncDataStream.unorderedWait(orderWideSpu, new DimAsyncFunction<OrderWide>("DIM_BASE_TRADEMARK") {

            @Override
            public String getKey(OrderWide obj) {
                return obj.getTm_id().toString();
            }

            @Override
            public void join(OrderWide obj, JSONObject dimInfoJsonObj) throws Exception {
                obj.setTm_name(dimInfoJsonObj.getString("TM_NAME"));
            }
        }, 60, TimeUnit.SECONDS);

        orderWideTmDS.print();
        //这个是json对象，传到kafka需要先转成String
        orderWideTmDS
                .map(
                        orderWide -> JSON.toJSONString(orderWide)
                )
                .addSink(MyKafkaUtil.getKafkaSink(orderWideSinkTopic));

        env.execute();
    }
}
