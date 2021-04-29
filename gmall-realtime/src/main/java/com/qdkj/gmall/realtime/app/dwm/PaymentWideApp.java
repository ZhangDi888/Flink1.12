package com.qdkj.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.qdkj.gmall.realtime.bean.OrderWide;
import com.qdkj.gmall.realtime.bean.PaymentInfo;
import com.qdkj.gmall.realtime.bean.PaymentWide;
import com.qdkj.gmall.realtime.utils.DateTimeUtil;
import com.qdkj.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class PaymentWideApp {
    public static void main(String[] args) throws Exception{

        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.0设置并行度
        env.setParallelism(4);

        //2.1设置检查点
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop101:9820/gmall/checkpoint/"));

        //3.创建消费的主题以及消费者组，以及发送的sink
        String orderWideSourceTopic = "dwm_order_wide";
        String dwdPaymentInfoSourceTopic = "dwd_payment_info";
        String groupId = "paymentWideApp";
        String dwmPaymentInfoSinkTopic = "dwm_payment_wide";

        //4.从需要的主题消费并把消费的数据转化成pojo
        FlinkKafkaConsumer<String> orderWideSource = MyKafkaUtil.getKafakaSource(orderWideSourceTopic, groupId);
        DataStreamSource<String> orderJsonStrDS = env.addSource(orderWideSource);
        //对数据进行转换结构 JsonString -> pojo
        SingleOutputStreamOperator<OrderWide> orderPojoDS = orderJsonStrDS.map(jsonStr -> JSON.parseObject(jsonStr, OrderWide.class));

        FlinkKafkaConsumer<String> paymentInfoSource = MyKafkaUtil.getKafakaSource(dwdPaymentInfoSourceTopic, groupId);
        DataStreamSource<String> paymentInfoJsonStrDS = env.addSource(paymentInfoSource);
        //对数据进行转换结构 JsonString -> pojo
        SingleOutputStreamOperator<PaymentInfo> paymentInfoPojoDS = paymentInfoJsonStrDS.map(jsonStr -> JSON.parseObject(jsonStr, PaymentInfo.class));

        //4.1创建支付流的Watermark，设置时间字段
        SingleOutputStreamOperator<PaymentInfo> paymentInfoWithWaterMark = paymentInfoPojoDS.assignTimestampsAndWatermarks(
                //设置数据允许迟到的时间
                WatermarkStrategy.<PaymentInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<PaymentInfo>() {
                                    @Override
                                    public long extractTimestamp(PaymentInfo paymentInfo, long recordTimestamp) {
                                        //指定时间字段，因为时间字段是String格式需要转成毫秒数
                                        return DateTimeUtil.toTs(paymentInfo.getCallback_time());
                                    }
                                }
                        )
        );

        //4.2创建订单宽表的时间戳
        SingleOutputStreamOperator<OrderWide> orderWideWithWaterMark = orderPojoDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<OrderWide>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<OrderWide>() {
                                    @Override
                                    public long extractTimestamp(OrderWide orderWide, long l) {
                                        return DateTimeUtil.toTs(orderWide.getCreate_time());
                                    }
                                }
                        )
        );

        //4.3对流进行分组，然后进行双流join;(PaymentInfo::getOrder_id)意思是调用PaymentInfo类的getOrder_id方法
        KeyedStream<PaymentInfo, Long> paymentInfoKeyByDS = paymentInfoWithWaterMark.keyBy(PaymentInfo::getOrder_id);

        KeyedStream<OrderWide, Long> orderWideKeyByDS = orderWideWithWaterMark.keyBy(OrderWide::getOrder_id);

        SingleOutputStreamOperator<Object> paymengWideDS = paymentInfoKeyByDS
                .intervalJoin(orderWideKeyByDS)
                //join的时间范围，订单前半小时
                .between(Time.milliseconds(-1800), Time.milliseconds(0))
                .process(
                        new ProcessJoinFunction<PaymentInfo, OrderWide, Object>() {
                            @Override
                            public void processElement(PaymentInfo paymentInfo, OrderWide orderWide, Context context, Collector<Object> collector) throws Exception {
                                collector.collect(new PaymentWide(paymentInfo, orderWide));
                            }
                        }
                );

        //5.将流数据转成String,再存到kafka
        SingleOutputStreamOperator<String> paymengWideStrDS = paymengWideDS.map(paymengWide -> JSON.toJSONString(paymengWide));
        paymengWideStrDS.addSink(MyKafkaUtil.getKafkaSink(dwmPaymentInfoSinkTopic));

        //6.创建执行
        env.execute();
    }
}
