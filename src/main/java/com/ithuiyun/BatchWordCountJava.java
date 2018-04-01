package com.ithuiyun;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * wordCount
 * 批处理程序
 * 读取一个或者多个文件中的数据，对数据进行分组求和
 *
 * Created by ithuiyun.com on 2018/4/1.
 */
public class BatchWordCountJava {

    public static void main(String[] args) throws Exception {
        String inputPath = "D:\\data\\file";
        String outPath = "D:\\data\\result";

        // 获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //获取数据源
        DataSet<String> text = env.readTextFile(inputPath);

        DataSet<Tuple2<String, Integer>> counts = text.flatMap(new Tokenizer())// hello world  ---> (hello,1)  (world,1)
                .groupBy(0)
                .sum(1);
        counts.writeAsCsv(outPath,"\n"," ");
                //.setParallelism(1);

        //执行
        env.execute("batch word count");

    }

    public static class Tokenizer implements FlatMapFunction<String,Tuple2<String,Integer>>{
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] tokens = value.toLowerCase().split("\\W+");
            for (String word:tokens) {
                if(word.length()>0){
                    out.collect(new Tuple2<String, Integer>(word,1));
                }
            }
        }
    }

}
