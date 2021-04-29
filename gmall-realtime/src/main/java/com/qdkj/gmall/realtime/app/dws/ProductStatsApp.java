package com.qdkj.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.qdkj.gmall.realtime.bean.OrderWide;
import com.qdkj.gmall.realtime.bean.PaymentWide;
import com.qdkj.gmall.realtime.bean.ProductStats;
import com.qdkj.gmall.realtime.common.GmallConstant;
import com.qdkj.gmall.realtime.func.DimAsyncFunction;
import com.qdkj.gmall.realtime.utils.ClickhouseUtil;
import com.qdkj.gmall.realtime.utils.DateTimeUtil;
import com.qdkj.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

public class ProductStatsApp {
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

        //TODO 2.从Kafka中获取数据流
        //2.1 声明相关的主题名称以及消费者组
        String groupId = "product_stats_app";
        String pageViewSourceTopic = "dwd_page_log";
        String favorInfoSourceTopic = "dwd_favor_info";
        String cartInfoSourceTopic = "dwd_cart_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSourceTopic = "dwm_payment_wide";
        String refundInfoSourceTopic = "dwd_order_refund_info";
        String commentInfoSourceTopic = "dwd_comment_info";

        //2.2 从页面日志中获取点击和曝光数据
        FlinkKafkaConsumer<String> pageViewSource = MyKafkaUtil.getKafakaSource(pageViewSourceTopic, groupId);
        DataStreamSource<String> pageViewDStream = env.addSource(pageViewSource);

        //2.3 从dwd_favor_info中获取收藏数据
        FlinkKafkaConsumer<String> favorInfoSourceSouce = MyKafkaUtil.getKafakaSource(favorInfoSourceTopic, groupId);
        DataStreamSource<String> favorInfoDStream = env.addSource(favorInfoSourceSouce);

        //2.4 从dwd_cart_info中获取购物车数据
        FlinkKafkaConsumer<String> cartInfoSource = MyKafkaUtil.getKafakaSource(cartInfoSourceTopic, groupId);
        DataStreamSource<String> cartInfoDStream = env.addSource(cartInfoSource);

        //2.5 从dwm_order_wide中获取订单数据
        FlinkKafkaConsumer<String> orderWideSource = MyKafkaUtil.getKafakaSource(orderWideSourceTopic, groupId);
        DataStreamSource<String> orderWideDStream = env.addSource(orderWideSource);

        //2.6 从dwm_payment_wide中获取支付数据
        FlinkKafkaConsumer<String> paymentWideSource = MyKafkaUtil.getKafakaSource(paymentWideSourceTopic, groupId);
        DataStreamSource<String> paymentWideDStream = env.addSource(paymentWideSource);

        //2.7 从dwd_order_refund_info中获取退款数据
        FlinkKafkaConsumer<String> refundInfoSource = MyKafkaUtil.getKafakaSource(refundInfoSourceTopic, groupId);
        DataStreamSource<String> refundInfoDStream = env.addSource(refundInfoSource);

        //2.8 从dwd_order_refund_info中获取评价数据
        FlinkKafkaConsumer<String> commentInfoSource = MyKafkaUtil.getKafakaSource(commentInfoSourceTopic, groupId);
        DataStreamSource<String> commentInfoDStream = env.addSource(commentInfoSource);

        //对点击曝光封装成统一格式，pageViewDStream包含了点击和曝光，用这一个就可以了
        SingleOutputStreamOperator<ProductStats> productClickAndDisplay = pageViewDStream.process(
                new ProcessFunction<String, ProductStats>() {
                    @Override
                    public void processElement(String jsonStrObj, Context ctx, Collector<ProductStats> out) throws Exception {
                        //将String转成json对象好做处理
                        JSONObject jsonObj = JSON.parseObject(jsonStrObj);
                        JSONObject pagJsonObj = jsonObj.getJSONObject("page");
                        String pageId = pagJsonObj.getString("page_id");

                        if (pageId == null) {
                            System.out.println(">>>>>" + jsonObj);
                        }
                        //获取日志的时间戳
                        Long ts = jsonObj.getLong("ts");
                        //如果当前访问的页面是商品详情，认为该商品被点击了一次
                        if ("good_detail".equals(pageId)) {
                            //获取点击商品的id
                            Long skuId = pagJsonObj.getLong("item");
                            //封装一次点击操作,这个是构造者模式封装对象，通过lombok插件使用
                            ProductStats productStats = ProductStats.builder().sku_id(skuId).click_ct(1L).ts(ts).build();
                            out.collect(productStats);
                        }

                        JSONArray displays = jsonObj.getJSONArray("displays");
                        //如果displays不为空，说明有曝光日志
                        if (displays != null && displays.size() > 0) {
                            for (int i = 0; i < displays.size(); i++) {
                                //获取曝光数据
                                JSONObject displayJsonObj = displays.getJSONObject(i);
                                //判断是否曝光的某一个商品
                                if ("sku_id".equals(displayJsonObj.getString("item_type"))) {
                                    //获取商品id
                                    Long skuId = displayJsonObj.getLong("item");
                                    ProductStats productStats = ProductStats.builder().sku_id(skuId).display_ct(1L).ts(ts).build();
                                    out.collect(productStats);
                                }
                            }
                        }


                    }
                }
        );

        //将订单的一些信息封装到ProductStats实体类
        SingleOutputStreamOperator<ProductStats> orderWideStatsDS = orderWideDStream.map(
                new MapFunction<String, ProductStats>() {
                    @Override
                    public ProductStats map(String jsonStr) throws Exception {
                        //先将str数据转换成实体类方便操作
                        OrderWide orderWide = JSON.parseObject(jsonStr, OrderWide.class);
                        String ct = orderWide.getCreate_time();
                        //把时间转换成时间戳，方便后面开窗
                        Long ts = DateTimeUtil.toTs(ct);
                        ProductStats productStats = ProductStats
                                .builder()
                                .sku_id(orderWide.getSku_id())
                                .order_sku_num(orderWide.getSku_num())
                                .order_amount(orderWide.getSplit_total_amount())
                                .ts(ts)
                                //将订单id统一放到一个set集合，方便统计订单数
                                .orderIdSet(new HashSet(Collections.singleton(orderWide.getOrder_id())))
                                .build();
                        return productStats;
                    }
                }
        );

        //将收藏数据转换成ProductStats格式
        SingleOutputStreamOperator<ProductStats> favorStatsDS = favorInfoDStream.map(
                new MapFunction<String, ProductStats>() {
                    @Override
                    public ProductStats map(String jsonStr) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        Long ts = DateTimeUtil.toTs(jsonObj.getString("create_time"));

                        ProductStats pro = ProductStats.builder()
                                .sku_id(jsonObj.getLong("sku_id"))
                                .favor_ct(1L)
                                .ts(ts)
                                .build();
                        return pro;
                    }
                }
        );

        //将购物车数据转换成ProductStats格式
        SingleOutputStreamOperator<ProductStats> cartStatsDS = cartInfoDStream.map(
                new MapFunction<String, ProductStats>() {
                    @Override
                    public ProductStats map(String jsonStr) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        //将字符串日期转成毫秒数
                        Long ts = DateTimeUtil.toTs(jsonObj.getString("create_time"));

                        ProductStats pro = ProductStats.builder()
                                .ts(ts)
                                .sku_id(jsonObj.getLong("sku_id"))
                                .cart_ct(1L)
                                .build();
                        return pro;
                    }
                }
        );

        //将支付数据转换成ProductStats格式
        SingleOutputStreamOperator<ProductStats> paymentStatsDS = paymentWideDStream.map(
                new MapFunction<String, ProductStats>() {
                    @Override
                    public ProductStats map(String jsonStr) throws Exception {
                        PaymentWide paymentWide = JSON.parseObject(jsonStr, PaymentWide.class);
                        //将字符串日期转成毫秒数
                        Long ts = DateTimeUtil.toTs(paymentWide.getPayment_create_time());

                        ProductStats pro = ProductStats.builder()
                                .ts(ts)
                                .sku_id(paymentWide.getSku_id())
                                .payment_amount(paymentWide.getSplit_total_amount())
                                .paidOrderIdSet(new HashSet(Collections.singleton(paymentWide.getOrder_id())))
                                .build();
                        return pro;
                    }
                }
        );

        //将退款数据转换成ProductStats格式
        SingleOutputStreamOperator<ProductStats> refundStatsDS = refundInfoDStream.map(
                new MapFunction<String, ProductStats>() {
                    @Override
                    public ProductStats map(String jsonStr) throws Exception {
                        JSONObject refundJsonObj = JSON.parseObject(jsonStr);
                        //将字符串日期转成毫秒数
                        Long ts = DateTimeUtil.toTs(refundJsonObj.getString("create_time"));

                        ProductStats pro = ProductStats.builder()
                                .sku_id(refundJsonObj.getLong("sku_id"))
                                .refund_amount(refundJsonObj.getBigDecimal("refund_amount"))
                                .refundOrderIdSet(new HashSet(Collections.singleton(refundJsonObj.getLong("order_id"))))
                                .ts(ts)
                                .build();
                        return pro;
                    }
                }
        );

        //将评价数据转换成ProductStats格式
        SingleOutputStreamOperator<ProductStats> commentStatsDS = commentInfoDStream.map(
                new MapFunction<String, ProductStats>() {
                    @Override
                    public ProductStats map(String jsonStr) throws Exception {
                        JSONObject commentJsonStr = JSON.parseObject(jsonStr);
                        //将字符串日期转成毫秒数
                        Long ts = DateTimeUtil.toTs(commentJsonStr.getString("create_time"));
                        //如果是好评就是1不是好评为0
                        long appraise = GmallConstant.APPRAISE_GOOD.equals(commentJsonStr.getString("appraise")) ? 1L : 0L;
                        ProductStats pro = ProductStats.builder()
                                .sku_id(commentJsonStr.getLong("sku_id"))
                                .comment_ct(1L)
                                .good_comment_ct(appraise)
                                .ts(ts)
                                .build();
                        return pro;
                    }
                }
        );

        //将几个流进行合并
        DataStream<ProductStats> unionDS = productClickAndDisplay.union(
                commentStatsDS,
                refundStatsDS,
                orderWideStatsDS,
                favorStatsDS,
                cartStatsDS,
                paymentStatsDS
        );

        //提取时间戳
        SingleOutputStreamOperator<ProductStats> productStatsWithTs = unionDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<ProductStats>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<ProductStats>() {
                                    @Override
                                    public long extractTimestamp(ProductStats productStats, long l) {
                                        return productStats.getTs();
                                    }
                                }
                        )
        );

        //对合并的流分组，然后进行开窗   开一个10s的滚动窗口
        WindowedStream<ProductStats, Long, TimeWindow> windowDS = productStatsWithTs.keyBy(
                new KeySelector<ProductStats, Long>() {
                    @Override
                    public Long getKey(ProductStats productStats) throws Exception {
                        return productStats.getSku_id();
                    }
                }
        ).window(
                TumblingEventTimeWindows.of(Time.seconds(10))
        );

        //对流数据进行聚合
        SingleOutputStreamOperator<ProductStats> reduceDS = windowDS.reduce(
                new ReduceFunction<ProductStats>() {
                    @Override
                    public ProductStats reduce(ProductStats stats1, ProductStats stats2) throws Exception {
                        stats1.setDisplay_ct(stats1.getDisplay_ct() + stats2.getDisplay_ct());
                        stats1.setClick_ct(stats1.getClick_ct() + stats2.getClick_ct());
                        stats1.setCart_ct(stats1.getCart_ct() + stats2.getCart_ct());
                        stats1.setFavor_ct(stats1.getFavor_ct() + stats2.getFavor_ct());
                        stats1.setOrder_amount(stats1.getOrder_amount().add(stats2.getOrder_amount()));
                        stats1.getOrderIdSet().addAll(stats2.getOrderIdSet());
                        stats1.setOrder_ct(stats1.getOrderIdSet().size() + 0L);
                        stats1.setOrder_sku_num(stats1.getOrder_sku_num() + stats2.getOrder_sku_num());

                        stats1.getRefundOrderIdSet().addAll(stats2.getRefundOrderIdSet());
                        stats1.setRefund_order_ct(stats1.getRefundOrderIdSet().size() + 0L);
                        stats1.setRefund_amount(stats1.getRefund_amount().add(stats2.getRefund_amount()));

                        stats1.getPaidOrderIdSet().addAll(stats2.getPaidOrderIdSet());
                        stats1.setPaid_order_ct(stats1.getPaidOrderIdSet().size() + 0L);
                        stats1.setPayment_amount(stats1.getPayment_amount().add(stats2.getPayment_amount()));

                        stats1.setComment_ct(stats1.getComment_ct() + stats2.getComment_ct());
                        stats1.setGood_comment_ct(stats1.getGood_comment_ct() + stats2.getGood_comment_ct());

                        return stats1;
                    }
                },
                new ProcessWindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
                    @Override
                    public void process(Long aLong, Context context, Iterable<ProductStats> iterable, Collector<ProductStats> collector) throws Exception {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        //对数据流进行遍历，填充窗口的创建，结束时间，以及这个流的创建时间
                        for (ProductStats productStats : iterable) {
                            productStats.setStt(sdf.format(new Date(context.window().getStart())));
                            productStats.setEdt(sdf.format(new Date(context.window().getEnd())));
                            productStats.setTs(new Date().getTime());
                            collector.collect(productStats);
                        }
                    }
                }
        );

        //进行维度关联，先关联sku
        SingleOutputStreamOperator<ProductStats> productStatsWithSkuDS = AsyncDataStream.unorderedWait(
                reduceDS,
                new DimAsyncFunction<ProductStats>("DIM_SKU_INFO") {
                    @Override
                    public String getKey(ProductStats productStats) {
                        return productStats.getSku_id().toString();
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject dimInfoJsonObj) throws Exception {
                        productStats.setSku_name(dimInfoJsonObj.getString("SKU_NAME"));
                        productStats.setSku_price(dimInfoJsonObj.getBigDecimal("PRICE"));
                        productStats.setSpu_id(dimInfoJsonObj.getLong("SPU_ID"));
                        productStats.setTm_id(dimInfoJsonObj.getLong("TM_ID"));
                        productStats.setCategory3_id(dimInfoJsonObj.getLong("CATEGORY3_ID"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        //进行维度关联，关联spu
        SingleOutputStreamOperator<ProductStats> productStatsWithSpuDS = AsyncDataStream.unorderedWait(
                productStatsWithSkuDS,
                new DimAsyncFunction<ProductStats>("DIM_SPU_INFO") {
                    @Override
                    public String getKey(ProductStats obj) {
                        return obj.getSpu_id().toString();
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject dimInfoJsonObj) throws Exception {
                        productStats.setSpu_name(dimInfoJsonObj.getString("SPU_NAME"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        //关联品牌维度
        SingleOutputStreamOperator<ProductStats> productStatsWithTMDS = AsyncDataStream.unorderedWait(
                productStatsWithSpuDS,
                new DimAsyncFunction<ProductStats>("DIM_BASE_TRADEMARK") {
                    @Override
                    public String getKey(ProductStats obj) {
                        return obj.getTm_id().toString();
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject dimInfoJsonObj) throws Exception {
                        productStats.setTm_name(dimInfoJsonObj.getString("TM_NAME"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        //关联品类维度
        SingleOutputStreamOperator<ProductStats> productStatsWithCategoryDS = AsyncDataStream.unorderedWait(
                productStatsWithTMDS,
                new DimAsyncFunction<ProductStats>("DIM_BASE_CATEGORY3") {
                    @Override
                    public String getKey(ProductStats obj) {
                        return obj.getCategory3_id().toString();
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject dimInfoJsonObj) throws Exception {
                        productStats.setCategory3_name(dimInfoJsonObj.getString("NAME"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        productStatsWithCategoryDS.print("productStatsWithCategoryDS>>>>>>>>>>>");
        //将聚合后的流数据写到ClickHouse中
        productStatsWithCategoryDS.addSink(
                ClickhouseUtil.<ProductStats>getJdbcSink("insert into product_stats_0821 values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
        );

        env.execute();
    }
}
