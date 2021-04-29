package com.qdkj.gmall.realtime.utils;

import com.qdkj.gmall.realtime.bean.TransientSink;
import com.qdkj.gmall.realtime.common.GmallConfig;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ClickhouseUtil {

    //获取针对clickhouse的jsbcSink
    public static <T>SinkFunction getJdbcSink(String sql) {
        SinkFunction<T> sinkFunction = JdbcSink.<T>sink(
                sql,
                new JdbcStatementBuilder<T>() {
                    //obj是流的一条数据，填充insert into visitor_stats_2021 values(?,?,?,?,?,?,?,?,?,?,?,?)占位符的值
                    @Override
                    public void accept(PreparedStatement ps, T obj) throws SQLException {
                        //通过反射获取类的所有属性（getDeclaredFields），包括私有的
                        Field[] declaredFields = obj.getClass().getDeclaredFields();
                        for (int i = 0; i < declaredFields.length; i++) {
                            Field field = declaredFields[i];
                            //设置私有属性可访问
                            field.setAccessible(true);

                            int count = 0;
                            //通过属性对象获取属性上是否有@TransientSink注解
                            TransientSink annotation = field.getAnnotation(TransientSink.class);
                            if (annotation != null) {
                                //如果属性标记了这个注解，说明这个字段要忽略
                                count++;
                                //跳过当前循环进入下一个
                                continue;
                            }

                            try {
                                //获取属性值，这个是反射操作方法，指的是这个字段通过这个类，可以获取到这个字段的属性值
                                Object o = field.get(obj);
                                //给占位符赋值，因为占位符索引从1开始，所以需要i + 1,如果忽略字段，就需要通过减count来更正插入值的顺序
                                ps.setObject(i+1-count,o);
                            } catch (IllegalAccessException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                },
                //构造者设计模式，创建JdbcExecutionOptions对象，给bachsize属性赋值，执行批次大小
                new JdbcExecutionOptions.Builder().withBatchSize(5).build(),
                //构造者设计模式,创建JdbcConnectionOptions对象,给连接相关的属性进行复制
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                        .build()
        );

        return sinkFunction;
    }
}
