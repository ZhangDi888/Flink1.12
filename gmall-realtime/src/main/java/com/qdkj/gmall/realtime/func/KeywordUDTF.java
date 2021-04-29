package com.qdkj.gmall.realtime.func;

import com.qdkj.gmall.realtime.utils.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * 自定义UDTF函数实现分词操作
 */
@FunctionHint(output = @DataTypeHint("ROW<word STRING"))
public class KeywordUDTF extends TableFunction<Row> {

    public void eval(String str) {
        //使用分词器,将字符串弄到list
        List<String> analyze = KeywordUtil.analyze(str);
        for (String word : analyze) {
            collect(Row.of(word));
        }
    }
}
