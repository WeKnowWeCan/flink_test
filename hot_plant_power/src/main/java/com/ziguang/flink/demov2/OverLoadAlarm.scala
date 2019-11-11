package com.ziguang.flink.demov2

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector


/*
需求2：
对负荷load过重达到某个临界值的发电机组及时预警
*/

//样例类，为模式匹配准备
case  class InLoad(powerId:Long,load:Long,timestamp:Long)

object OverLoadAlarm {
  def main(args: Array[String]): Unit = {
    //获取外部参数
    val parameters: ParameterTool = ParameterTool.fromArgs(args)
    val output_major: String = parameters.get("output_major")
    val output_side: String = parameters.get("output_side")

    //get运行流处理环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置时间语义为eventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    //设置并行度
    env.setParallelism(1)
    //读取输入流
    val inputDStream: DataStream[String] = env.socketTextStream("192.168.1.102",6666)
    //对流进行模式匹配 并 引入watermark
    val classDStream: DataStream[InLoad] = inputDStream
      .map(
           line => {
              val fields: Array[String] = line.split(",")
              InLoad(fields(0).toLong, fields(1).toLong, fields(2).toLong)
          }
      )
      .assignAscendingTimestamps(_.timestamp*1000)

    //利用process函数将特殊事件发送到侧输出流种
    val majorStream: DataStream[InLoad] = classDStream.process(new OverLoadingMonitor)
    majorStream.print("major_data")
    majorStream.writeAsText("output_major",WriteMode.OVERWRITE)

    val sideStream: DataStream[String] = majorStream.getSideOutput(OutputTag[String]("overloading_alarm"))
    sideStream.print("output_side")
    sideStream.writeAsText("output_side",WriteMode.OVERWRITE)
    env.execute("发电机组超负荷预警")
  }
}

//自定义ProcessFunction
class OverLoadingMonitor() extends  ProcessFunction[InLoad,InLoad]{
  //定义侧输出流的标签，之后使用
  val alarmOutput: OutputTag[String] = new OutputTag[String]("overloading_alarm")
  //重写方法processElement（i对应输入，context为运行环境，collector为整体输出）
  override def processElement(i: InLoad, context: ProcessFunction[InLoad, InLoad]#Context, collector: Collector[InLoad]): Unit = {

    //当某个发电机组的负载load超过阈值8888时，在侧输出流单独输出
    if(i.load > 8088.0){
      context.output(alarmOutput,s"overloading alarm for ${i.powerId}"+s"---load is ${i.load}")
    }else{
      //其他数据按照常规输出
      collector.collect(i)
    }

  }
}


