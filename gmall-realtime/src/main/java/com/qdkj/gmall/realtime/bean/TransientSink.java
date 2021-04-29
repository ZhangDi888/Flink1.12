package com.qdkj.gmall.realtime.bean;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;

/**
 * 用该注解标记的属性，不需要插入Clickhouse
 */
//这个值作用的地方，字段
@Target(FIELD)
//这个只作用的范围
@Retention(RetentionPolicy.RUNTIME)
public @interface TransientSink {

}
