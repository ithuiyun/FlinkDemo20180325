package com.ithuiyun

import org.apache.flink.api.scala.ExecutionEnvironment

/**
  * Created by ithuiyun.com on 2018/4/1.
  */
object BatchWordCountScala {
  def main(args: Array[String]): Unit = {
    val inputPath = "D:\\data\\file"
    val outPath = "D:\\data\\result2"

    val env = ExecutionEnvironment.getExecutionEnvironment
    val text = env.readTextFile(inputPath)

    import org.apache.flink.api.scala._
    val counts = text.flatMap(_.toLowerCase().split("\\W+"))
      .filter(_.nonEmpty)
      .map((_,1))
      .groupBy(0)
      .sum(1).setParallelism(1)

    counts.writeAsCsv(outPath,"\n"," ")
    env.execute("batch word count")
  }

}
