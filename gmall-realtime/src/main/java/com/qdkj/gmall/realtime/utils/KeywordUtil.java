package com.qdkj.gmall.realtime.utils;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * IK分词器
 */
public class KeywordUtil {

    //分词，将传进来的String字符串，进行智能分词，放进list
    public static List<String> analyze(String text) {

        List<String> wordList = new ArrayList<>();
        //将字符串转成字符输入流
        StringReader sr = new StringReader(text);
        //创建分词对象
        IKSegmenter ikSegmenter = new IKSegmenter(sr, true);
        Lexeme lexeme = null;
        while (true) {
            try {
                //获取一个单词
                if ((lexeme = ikSegmenter.next()) != null) {
                    String lexemeText = lexeme.getLexemeText();
                    wordList.add(lexemeText);
                } else {
                    break;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return wordList;
    }

/*    public static void main(String[] args) {
        String text = "Apple iPhoneXSMax (A2104) 256GB 深空灰色 移动联通电信4G手机 双卡双待";
        System.out.println(KeywordUtil.analyze(text));
    }*/
}
