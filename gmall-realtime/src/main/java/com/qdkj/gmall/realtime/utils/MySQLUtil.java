package com.qdkj.gmall.realtime.utils;

import com.google.common.base.CaseFormat;
import com.qdkj.gmall.realtime.bean.TableProcess;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class MySQLUtil {

    /**
     * @param sql 执行的sql语句
     * @param clz  返回的数据类型
     * @param underScoreToCamel  是否将下划线转换为驼峰命名法
     * @param <T>
     * @return
     */
    public static<T> List<T> queryList(String sql, Class<T> clz, boolean underScoreToCamel){

        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            //1.注册驱动
            Class.forName("com.mysql.jdbc.Driver");
            //2.创建连接
            con = DriverManager.getConnection("jdbc:mysql://192.168.10.101:3306/gmall2021_realtime?characterEncoding=utf-8&useSSL=false",
                    "root",
                    "123456");
            //3.创建数据库操作对象
            ps = con.prepareStatement(sql);
            //4.执行sql语句
            // 100    zd    20
            // 200    ls    60
            rs = ps.executeQuery();
            //查询结果的元数据信息
            // id    student_name    age
            ResultSetMetaData metaData = rs.getMetaData();
            List<T> resultList = new ArrayList<T>();

            //5.处理结果集
            //判断结果集是否存在数据,如果有,那么进行一次循环
            while(rs.next()){
                //创建一个对象,用于封装查询出来一条结果集的数据
                //通过反射这种方式创建对象
                T obj = clz.newInstance();

                //对于jdbc的操作,他的索引都是从1开始的; 对查询的每一列进行遍历,获取每一列的名称
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    String columnName = metaData.getColumnName(i);
                    String propertyName = "";
                    //如果指定将下划线转换成驼峰命名法的值为true, 通过guava工具类,将表中的列转换为类属性的驼峰命名法的形式
                    if (underScoreToCamel) {
                        propertyName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                    }
                    //调用apache的commons-bean中工具类,给obj属性赋值
                    BeanUtils.setProperty(obj, propertyName, rs.getObject(i));
                }
                resultList.add(obj);
            }
            return resultList;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("从MySQL查询数据失败");
        } finally {
            //6.释放资源
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

            if (con != null) {
                try {
                    con.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    public static void main(String[] args) {
        List<TableProcess> list = queryList("select * from table_process", TableProcess.class, true);
        for (TableProcess tableProcess : list) {
            System.out.println(tableProcess);
        }
    }
}
