package com.qdkj.gmall.realtime.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class MyKafkaUtil {

    public static String KAFKA_SERVER = "hadoop101:9092,hadoop102:9092,hadoop103:9092";
    public static String DEFAULT_TOPIC = "DEFAULT_DATA";

    //获取FlinkKafkaConsumer
    public static FlinkKafkaConsumer<String> getKafakaSource(String topic, String groupid) {
        //Kafka连接的一些属性设置
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupid);
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,KAFKA_SERVER);
        return new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(),props);
    }

    //封装kafka生产者
    public static FlinkKafkaProducer<String> getKafkaSink(String topic) {
        return new FlinkKafkaProducer<String>(KAFKA_SERVER,topic,new SimpleStringSchema());
    }

    //自定义kafka序列化，这样就可以传json类型的数据，之前的生产者只能传String类型
    public static <T>FlinkKafkaProducer<T> getKafkaSinkBySchema(KafkaSerializationSchema<T> kafkaSerializationSchema) {
        //kafka基本信息
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        //设置kafka生产者超时时间为15分钟
        props.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,15*60*1000 +"");
        //kafka基本配置信息
        return new FlinkKafkaProducer<T>(DEFAULT_TOPIC, kafkaSerializationSchema, props, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }

    //拼接Kafka相关属性到DDL
    public static String getKafkaDDL(String topic,String groupId){
        String ddl="'connector' = 'kafka', " +
                " 'topic' = '"+topic+"',"   +
                " 'properties.bootstrap.servers' = '"+ KAFKA_SERVER +"', " +
                " 'properties.group.id' = '"+groupId+ "', " +
                "  'format' = 'json', " +
                "  'scan.startup.mode' = 'latest-offset'  ";
        return  ddl;
    }
}
