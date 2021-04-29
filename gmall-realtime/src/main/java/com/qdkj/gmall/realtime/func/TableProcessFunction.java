package com.qdkj.gmall.realtime.func;

import com.alibaba.fastjson.JSONObject;
import com.qdkj.gmall.realtime.bean.TableProcess;
import com.qdkj.gmall.realtime.common.GmallConfig;
import com.qdkj.gmall.realtime.utils.MySQLUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

public class TableProcessFunction extends ProcessFunction<JSONObject, JSONObject> {

    //声明phoenix链接
    Connection connection = null;

    //用于在内存中存放配置表信息的Map<表名：操作类型，tableprocess>
    private Map<String,TableProcess> tableProcessmap = new HashMap<>();

    //用于存放已经在phoenix的表名，因为表名不能重复，用set存放，并去重
    private Set<String> setTable = new HashSet();

    //因为要将维度数据输出到侧输出流，所以我们在这里定义一个侧输出流标记
    private OutputTag<JSONObject> outputTag;

    public TableProcessFunction(OutputTag<JSONObject> outputTag) {
        this.outputTag = outputTag;
    }

    @Override
    public void open(Configuration parameters) throws Exception {

        //初始化phoenix链接
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        //每个并行度执行都只会启动这个方法，去同步mysql的表将数据
        refreshMeta();
        //定时去更新mysql数据库同步到map中;从现在起过delay毫秒后，每隔period毫秒执行一次
        Timer timer = new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                //调用这个方法去同步
                refreshMeta();
            }
        },5000,5000);
    }

    //周期性查询配置表
    private void refreshMeta() {

        //============1.查询mysql配置表===============
        List<TableProcess> tableProcesses = MySQLUtil.queryList("select * from table_process", TableProcess.class, true);
        //对表记录进行遍历
        for (TableProcess tableProcess : tableProcesses) {
            //获取一行记录的表名
            String sourceTableName = tableProcess.getSourceTable();
            //获取一行记录的表的操作类型
            String operateType = tableProcess.getOperateType();
            //获取一行记录的表的输出类型
            String sinkType = tableProcess.getSinkType();
            //获取一行记录输出到hbase表或者主题名
            String sinkTable = tableProcess.getSinkTable();
            //获取一行记录的表的输出字段
            String sinkColumns = tableProcess.getSinkColumns();
            //获取一行记录的表的主键字段
            String sinkPk = tableProcess.getSinkPk();
            //获取一行记录的表的建表扩展
            String sinkExtend = tableProcess.getSinkExtend();

            //============2.同步到内存中===============
            //制作map的key 表名+操作类型
            String key = sourceTableName + ":" + operateType;
            //将这条记录保存到内存中，也就是map中
            tableProcessmap.put(key, tableProcess);

            //============3.如果当前配置是维度表，需要向hbase保存数据，那么需要检查phoenix中是否存在这张表，如果不存在创建表===============
            if (TableProcess.SINK_TYPE_HBASE.equals(sinkType) && "insert".equals(operateType)) {

                boolean notExist = setTable.add(sourceTableName);
                //如果是true，说明之前没创建过表，需要在phoenix中创建表
                if (notExist) {
                    //检查phoenix表是否存在
                    //有可能应用被重启，导致保存在内存数据被清空，但是实际phoenix已经创建过表
                    //检查表
                    checkTable(sinkTable, sinkColumns, sinkPk, sinkExtend);
                }
            }

            //如果没有从数据库读取到数据
            if (tableProcessmap == null || tableProcessmap.size() == 0) {
                throw new RuntimeException("没有从数据库的配置表中读取到数据");
            }

        }


    }

    private void checkTable(String sourceTableName, String filelds, String pk, String ext) {
        //如果在配置表中，没有配置主键，需要给一个默认的值
        if (pk == null) {
            pk = "id";
        }
        //如果在配置表中，没有配置建表扩展，需要给一个默认的值
        if (ext == null) {
            ext = "";
        }

        //拼接建表语句 命名空间 + 表名
        StringBuilder createTable = new StringBuilder("create table if not exists "
        + GmallConfig.HBASE_SCHEMA + "." + sourceTableName + "(");

        //创建的字段在一个列中，用“,”分开，所以需要切分，形式数组，然后遍历，判断哪个是主键，同时，建表的时候，如果不是最后一个字段的话，需要添加“,”
        String[] fileldsArr = filelds.split(",");

        for (int i = 0; i < fileldsArr.length; i++) {
            String field = fileldsArr[i];

            //判断当前字段是否为主键字段
            if (pk.equals(field)) {
                createTable.append(field).append(" varchar primary key ");
            } else {
                createTable.append("info.").append(field).append(" varchar ");
            }
            if (i < fileldsArr.length - 1) {
                createTable.append(",");
            }

        }

        createTable.append(")");
        createTable.append(ext);

        PreparedStatement ps = null;

        try {
            //上面已经创建好phoenix连接，执行sql
            ps = connection.prepareStatement(createTable.toString());
            ps.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                ps.close();
            } catch (SQLException e) {
                e.printStackTrace();
                throw new RuntimeException("phoenix建表失败");
            }
        }

    }

    //这个方法最关键，每过来一个元素，方法执行一次，主要任务是根据内存中的配置表，对当前数据进行分流处理
    @Override
    public void processElement(JSONObject jsonObject, Context context, Collector<JSONObject> out) throws Exception {
        //数据的表名
        String tableName = jsonObject.getString("table");
        //数据的操作类型
        Object type = jsonObject.get("type");
        //数据修复，如果maxwell的Bootstrap历史同步数据的话，他的操作类型是bootstrap-insert
        if ("bootstrap-insert".equals(type)) {
            //如果同步的是历史数据的话,就把操作类型改成insert
            type = "insert";
            jsonObject.put("type", type);
        }
        //如果map中有数据，才去查询，没有就不查
        if (tableProcessmap != null && tableProcessmap.size() > 0) {
            //拼接key去查询配置表
            String key = tableName + ":" + type;
            TableProcess tableProcess = tableProcessmap.get(key);

            //获取元素对应的配置信息
            if (tableProcess != null) {
                //获取sinkTable, 指明当前数据发往何处，如果是维度数据就发往hbase，如果是事实数据就发往kafka
                jsonObject.put("sink_table",tableProcess.getSinkTable());
                //如果指明了保留哪些字段，需要对这些字段进行过滤
                if (tableProcess.getSinkColumns() != null && tableProcess.getSinkColumns().length() > 0) {
                    //根据配置表需要的字段，过滤掉不用的字段
                    filterColumn(jsonObject.getJSONObject("data"), tableProcess.getSinkColumns());
                }
            } else {
                System.out.println("  " + key + "in MysQL");
            }

            //根据sinktype来决定数据将发往哪个地方
            if (tableProcess != null && tableProcess.getSinkType().equals(TableProcess.SINK_TYPE_HBASE)) {
                //如果类型是hbase就发往hbase
                context.output(outputTag, jsonObject);
            } else if (tableProcess != null && tableProcess.getSinkType().equals(TableProcess.SINK_TYPE_KAFKA)){
                //如果类型是kafka就发往kafka
                out.collect(jsonObject);
            }
        }

    }

    private void filterColumn(JSONObject data, String sinkColumns) {
        //sinkColumns要保留哪些列 id,out_trade_no,order_id
        String[] fieldArr = sinkColumns.split(",");

        //将数据转为list，为了判断集合中是否包含某个元素
        List<String> columnList = Arrays.asList(fieldArr);

        //获取json对象中封装的一个个键值对，每个键值对封装为Entry类型
        Set<Map.Entry<String, Object>> entrieSet = data.entrySet();

        //通过迭代器对不需要的键值对进行移除
        Iterator<Map.Entry<String, Object>> iterator = entrieSet.iterator();

        for (;iterator.hasNext();) {
            //对迭代器进行迭代
            Map.Entry<String, Object> entry = iterator.next();
            //如果当前键值对的key不在需要的字段里面，就使用迭代器移除
            if (!columnList.contains(entry.getKey())) {
                iterator.remove();
            }
        }

    }


}
