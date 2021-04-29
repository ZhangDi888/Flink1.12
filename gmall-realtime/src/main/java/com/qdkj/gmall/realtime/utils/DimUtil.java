package com.qdkj.gmall.realtime.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;
import redis.clients.jedis.Jedis;

import java.util.List;

public class DimUtil {
    public static JSONObject getDimInfoNoCache(String tableName, Tuple2<String, String> ... cloNameAndValue){

        String whereSql = " where ";

        for (int i = 0; i < cloNameAndValue.length; i++) {
            Tuple2<String, String> stringStringTuple2 = cloNameAndValue[i];
            String filedName = stringStringTuple2.f0;
            String fileValue = stringStringTuple2.f1;
            if (i > 0) {
                whereSql += " and ";
            }
            whereSql += filedName + "='" + fileValue + "'";
        }

        String sql = "select * from " + tableName + whereSql;

        List<JSONObject> jsonObjects = PhoenixUtil.queryList(sql, JSONObject.class);
        JSONObject jsonObject = null;

        if (jsonObjects != null && jsonObjects.size() > 0) {
                jsonObject = jsonObjects.get(0);
        } else {
            System.out.println("没有从维度表查询到相关数据" + sql);
        }

        return jsonObject;
    }

    public static JSONObject getDimInfo(String tableName, String id) {
        return getDimInfo(tableName,Tuple2.of("id",id));
    }
    /**
     * redis value值类型选择String
     * key设计：dim:表名:a_b   a_b为多个查询条件,比如条件id=10，a就是10，如果只有一个的话，就是dim:表名:a
     *
     */

    public static JSONObject getDimInfo(String tableName, Tuple2<String, String> ... cloNameAndValue){

        String whereSql = " where ";
        String redisKey = "dim:" + tableName.toLowerCase() + ":";

        for (int i = 0; i < cloNameAndValue.length; i++) {
            Tuple2<String, String> stringStringTuple2 = cloNameAndValue[i];
            String filedName = stringStringTuple2.f0;
            String fileValue = stringStringTuple2.f1;
            if (i > 0) {
                whereSql += " and ";
                redisKey += "_";
            }
            whereSql += filedName + "='" + fileValue + "'";
            redisKey += fileValue;
        }

        //从redis获取数据
        Jedis jedis = null;
        //维度数据的json字符串形式
        String dimJsonStr = null;
        //维度数据的json对象形式
        JSONObject dimJsonObj = null;

        try {
            //获取jedis客户端
            jedis = RedisUtil.getJedis();
            //根据key查询Redis
            dimJsonStr = jedis.get(redisKey);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("从redis中查询维度失败");
        }
        //如果查询有值，String转obj
        if (dimJsonStr != null && dimJsonStr.length() > 0) {
            dimJsonObj = JSON.parseObject(dimJsonStr);
        } else {
            //如果从redis查不到数据，就从phoenix查询
            String sql = "select * from " + tableName + whereSql;
            System.out.println("查询维度的SQL:" + sql);
            List<JSONObject> jsonObjects = PhoenixUtil.queryList(sql, JSONObject.class);
            //对于维度查询来讲，一般都是根据主键进行查询，不可能返回多条记录，只会有一条
            if (jsonObjects != null && jsonObjects.size() > 0) {
                dimJsonObj = jsonObjects.get(0);
                //就把值在
                if (jedis != null) {
                    jedis.setex(redisKey, 3600*24,dimJsonObj.toJSONString());
                }
            } else {
                System.out.println("没有从维度表查询到相关数据" + sql);
            }

        }

        //关闭redis
        if (jedis != null) {
            jedis.close();
        }

        return dimJsonObj;
    }

    public static void deleteCached(String tableName, String id) {
        String key = "dim:" + tableName.toLowerCase() + ":" + id;
        try {
            Jedis jedis = RedisUtil.getJedis();
            //通过key清楚缓存
            jedis.del(key);
            jedis.close();
        } catch (Exception e) {
            System.out.println("缓存异常！");
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
//        JSONObject dimInfoNoCache = getDimInfoNoCache("BASE_TRADEMARK", Tuple2.of("id", "18"));
        JSONObject base_trademark = DimUtil.getDimInfo("BASE_TRADEMARK", "1");
        System.out.println(base_trademark);
    }
}
