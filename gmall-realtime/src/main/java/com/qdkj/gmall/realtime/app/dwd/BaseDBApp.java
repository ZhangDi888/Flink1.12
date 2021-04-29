package com.qdkj.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.qdkj.gmall.realtime.bean.TableProcess;
//import com.qdkj.gmall.realtime.func.DimSink;
import com.qdkj.gmall.realtime.func.DimSink;
import com.qdkj.gmall.realtime.func.TableProcessFunction;
import com.qdkj.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class BaseDBApp {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置平行度，这个要和kafka分区保持一致
        env.setParallelism(4);

        //1.设置检查点的频率
        env.enableCheckpointing(5000);
        //1.1设置检查点超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        //1.2设置存储的地方
        env.setStateBackend(new FsStateBackend("hdfs://hadoop101:9820/gmall/checkpoint/baseDbApp"));
        //重启策略
        //如果没有开启重启checkpoint，那么重启策略就是noRestart
        //如果说没有开checkpoint, 那么重启策略会尝试自动帮你进行重启， 重启次数Integer.MaxValue
        //这样的话，如果出现异常，打印台就能输出异常信息
//        env.setRestartStrategy(RestartStrategies.noRestart());

        //2.从kafka读取数据
        //2.1定义要消费的topic，以及消费者
        String odsDbTopic = "ods_base_db_m";
        String groupIdDb = "groupIdDb";

        FlinkKafkaConsumer<String> dbSource = MyKafkaUtil.getKafakaSource(odsDbTopic, groupIdDb);
        DataStreamSource<String> jsonStreamSource = env.addSource(dbSource);

        //对数据进行转换结构 String --> json
        SingleOutputStreamOperator<JSONObject> jsonData = jsonStreamSource.map(data -> JSON.parseObject(data));
        //这种也是解析json方式,意思是调用JSON的parseObject对数据进行转换
//        SingleOutputStreamOperator<JSONObject> jsonData = jsonStreamSource.map(JSON::parseObject);

        //对数据进行过滤 如果表明不为空而且data对象不为空,并且data长度大于3, 就将数据保留, flag为true的数据留下
        SingleOutputStreamOperator<JSONObject> filterData = jsonData.filter(
                Data -> {

                    boolean flag = Data.getString("table") != null
                            //这个是获取对象的方法
                            && Data.getJSONObject("data") != null
                            //这个是获取字符串的方法
                            && Data.getString("data").length() > 3;

                    return flag;
                }
        );

        //将数据分流，维度表发送到hbase，事实表发送到kafka
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>(TableProcess.SINK_TYPE_HBASE){};

        //主流写到kafka
        SingleOutputStreamOperator<JSONObject> kafkaDS = filterData.process(
                new TableProcessFunction(hbaseTag)
        );

        //获取侧输出流，写到hbase
        DataStream<JSONObject> hbaseDS = kafkaDS.getSideOutput(hbaseTag);

        kafkaDS.print("事实>>>>>>>>");
        hbaseDS.print("维度>>>>>>>>");

        hbaseDS.addSink(new DimSink());
        //实现kafka自定义序列化，根据主题名发送到kafka各个主题
        FlinkKafkaProducer<JSONObject> kafkaSink = MyKafkaUtil.getKafkaSinkBySchema(
                new KafkaSerializationSchema<JSONObject>() {

                    @Override
                    public void open(SerializationSchema.InitializationContext context) throws Exception {
                        System.out.println("kafka序列化");
                    }

                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObject, @Nullable Long aLong) {
                        //获取发往的主题名
                        String sinkTopic = jsonObject.getString("sink_table");
                        //获取发送的数据
                        JSONObject dataObj = jsonObject.getJSONObject("data");
                        return new ProducerRecord<>(sinkTopic, dataObj.toString().getBytes());
                    }
                }
        );
        kafkaDS.addSink(kafkaSink);
        env.execute();
    }
}
