package com.ziguang.flink.demov2

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
/*需求1：
  统计各机组各个月的发电总量
*/

//样例类，为后面模式匹配使用
case class MonPower(month:Long,powerId:Long,plantPower:Long)

object GrossPowerAnalysis {
  def main(args: Array[String]): Unit = {
    //获取外部参数
    val parameters: ParameterTool = ParameterTool.fromArgs(args)
    val input: String = parameters.get("input")
    val output: String = parameters.get("output")
    //get运行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //读入数据
    val power_day_inputDS: DataSet[String] = env.readTextFile("input")
    //val power_day_inputDS: DataSet[String] = env.readTextFile("F:\\develop\\demo_v2\\hot_plant_power\\src\\main\\resources\\powerofday")
    //将数据匹配为类信息
    val classDS: DataSet[MonPower] = power_day_inputDS.map(
      line => {
        val fields: Array[String] = line.split(",")
        MonPower(fields(0).substring(0,6).toLong, fields(1).toLong, fields(2).toLong)
      }
    )
    //分组求和操作
    val sumDS: AggregateDataSet[MonPower] = classDS.groupBy("month","powerId").sum(2)
    val outputDS: DataSet[String] = sumDS.map(
      a => {
        a.month + "," + a.powerId + "," + a.plantPower
      }
    )
    //打印输出
    outputDS.print()
    //存为text文件
    outputDS.writeAsText("output",WriteMode.OVERWRITE)
    env.execute("grossPower")

  }

}
