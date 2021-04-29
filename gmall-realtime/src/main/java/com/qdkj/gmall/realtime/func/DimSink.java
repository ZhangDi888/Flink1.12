package com.qdkj.gmall.realtime.func;

import com.alibaba.fastjson.JSONObject;
import com.qdkj.gmall.realtime.bean.TableProcess;
import com.qdkj.gmall.realtime.common.GmallConfig;
import com.qdkj.gmall.realtime.utils.DimUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

public class DimSink extends RichSinkFunction<JSONObject> {

    //定义phoenix连接对象
    private Connection conn = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        //注册phoenix驱动
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        //获取连接
        conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

/**
     * 对流中数据进行处理
     * @param jsonObj
     * @param context
     * @throws Exception
     */

    @Override
    public void invoke(JSONObject jsonObj, Context context) throws Exception {
        //获取要发往的表名或者主题
        String sinkTable = jsonObj.getString("sink_table");
        //获取json中的data数据，这个数据是经过过滤之后保留的业务表字段
        JSONObject dataJson = jsonObj.getJSONObject("data");

        if (dataJson != null && dataJson.size() > 0) {
            //根据data中属性名和属性值，生成upsert语句
            String upsertSql = genUpsertSql(sinkTable.toUpperCase(), dataJson);
            System.out.println("向phoenix插入数据的sql" + upsertSql);

            PreparedStatement ps = null;
            try {
                //执行sql
                ps = conn.prepareStatement(upsertSql);
                ps.execute();
                //注意，执行完phoenix需要手动提交事务，mysql的话他是自动提交的
                conn.commit();
            } catch (SQLException e) {
                e.printStackTrace();
                System.out.println("向phoenix插入数据失败");
            } finally {
                if (ps != null) {
                    ps.close();
                }
            }
            //如果当前做的是更新操作，需要将Redis中缓存的数据清除掉
            if (jsonObj.getString("type").equals("update")) {
                DimUtil.deleteCached(sinkTable,dataJson.getString("id"));
            }
        }

    }

    //根据data属性和值，生成向phoenix中插入数据的sql语句
    private String genUpsertSql(String tableName, JSONObject dataJson) {
        //"data":{"id":80,"tm_name":"xiaomi"}
        //upsert into 表空间.表名(列名...)values(值..)
        //将json的key全部取出来
        Set<String> keys = dataJson.keySet();
        //取出json的所有value
        Collection<Object> values = dataJson.values();

        String upsertSql = "upsert into " + GmallConfig.HBASE_SCHEMA + "." + tableName + "(" +
                //通过这个join方法可以将key数据按照“，”去拼接
                StringUtils.join(keys, ",") + ")";

        //通过join方法将value的值用“,”拼接，形成values ('A','B','C')这种形式
        String valueSql = " values ('" + StringUtils.join(values, "','") + "')";

        return upsertSql + valueSql;
    }
}
