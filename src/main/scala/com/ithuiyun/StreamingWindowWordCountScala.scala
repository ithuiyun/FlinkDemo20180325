package com.ithuiyun

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

/**
  *
  * 手工模拟通过socket实时产生一些数据
  * 使用flink对指定窗口内的数据进行实时统计
  * 最终把结果打印出来
  *
  * 注意：代码执行之前需要做两部操作
  * 1：先把pom中的依赖做一下修改，去掉scope配置
  * 2：先在hadoop100机器上执行nc -l 9000
  * Created by ithuiyun.com on 2018/3/25.
  */
object StreamingWindowWordCountScala {

  def main(args: Array[String]): Unit = {
    // 获取port 参数
    val port: Int = try{
      ParameterTool.fromArgs(args).getInt("port")
    }catch {
      case e: Exception=>{
        System.err.println("没有指定端口号，使用默认端口9000")
      }
        9000
    }

    // 获取运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //链接socket获取数据源
    val text = env.socketTextStream("hadoop100",port,'\n')

    //解析数据，分组，窗口操作，聚合求sum
    //注意：在这需要做一个隐式转换，否则使用flatmap会报错
    import org.apache.flink.api.scala._
    val windowCount = text.flatMap(line=>line.split("\\s"))
      .map(word=>WordWithCount(word,1L))
      .keyBy("word")
      .timeWindow(Time.seconds(2),Time.seconds(1))
      .sum("count")
      //.reduce((a,b)=>WordWithCount(a.word,a.count+b.count))

    //使用一个单线程打印结果
    windowCount.print().setParallelism(1)

    env.execute("streaming word count")

  }

  case class WordWithCount(word: String,count: Long)
}
