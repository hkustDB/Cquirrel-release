import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.hkust.RelationType.Payload
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.api.common.serialization.SimpleStringSchema
object Job {
   val lineitemTag: OutputTag[Payload] = OutputTag[Payload]("lineitem")
   val ordersTag: OutputTag[Payload] = OutputTag[Payload]("orders")
   def main(args: Array[String]) {
      val env = StreamExecutionEnvironment.getExecutionEnvironment
      val params: ParameterTool = ParameterTool.fromArgs(args)
      env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
      var executionConfig = env.getConfig
      executionConfig.enableObjectReuse()
      val inputpath = "file:///aju/q3flinkInput.csv"
      val outputpath = "file:///aju/q3flinkOutput.csv"
      val inputStream : DataStream[Payload] = getStream(env,inputpath)
      val orders : DataStream[Payload] = inputStream.getSideOutput(ordersTag)
      val lineitem : DataStream[Payload] = inputStream.getSideOutput(lineitemTag)
      val ordersS = orders.keyBy(i => i._3)
      .process(new Q4OrdersProcessFunction())
      val lineitemS = ordersS.connect(lineitem)
      .keyBy(i => i._3, i => i._3)
      .process(new Q4LineitemProcessFunction())
      val result = lineitemS.keyBy(i => i._3)
      .process(new Q4Aggregate1ProcessFunction())
      .map(x => Payload("Aggregate", "Addition", x._3, x._4, x._5, x._6))
      .keyBy(i => i._3)
      .process(new Q4Aggregate2ProcessFunction())
      .map(x => (x._4.mkString(", "), x._5.mkString(", "), x._6))
      result.map(x => x.toString()).writeToSocket("localhost",5001,new SimpleStringSchema())
      result.writeAsText(outputpath,FileSystem.WriteMode.OVERWRITE)
      .setParallelism(1)
      env.execute("Flink Streaming Scala API Skeleton")
   }
   private def getStream(env: StreamExecutionEnvironment, dataPath: String): DataStream[Payload] = {
      val data = env.readTextFile(dataPath).setParallelism(1)
      val format = new java.text.SimpleDateFormat("yyyy-MM-dd")
      var cnt : Long = 0
      val restDS : DataStream[Payload] = data
      .process((value: String, ctx: ProcessFunction[String, Payload]#Context, out: Collector[Payload]) => {
      val header = value.substring(0,3)
      val cells : Array[String] = value.substring(3).split("\\|")
      var relation = ""
      var action = ""
      header match {
         case "+LI" =>
         action = "Insert"
         relation = "lineitem"
         val i = Tuple4(format.parse(cells(11)),cells(0).toLong,cells(3).toInt,format.parse(cells(12)))
         cnt = cnt + 1
         ctx.output(lineitemTag, Payload(relation, action, cells(0).toLong.asInstanceOf[Any],
         Array[Any](i._1,i._2,i._3,i._4),
         Array[String]("L_COMMITDATE","ORDERKEY","LINENUMBER","L_RECEIPTDATE"), cnt))
         case "-LI" =>
         action = "Delete"
         relation = "lineitem"
         val i = Tuple4(format.parse(cells(11)),cells(0).toLong,cells(3).toInt,format.parse(cells(12)))
         cnt = cnt + 1
         ctx.output(lineitemTag, Payload(relation, action, cells(0).toLong.asInstanceOf[Any],
         Array[Any](i._1,i._2,i._3,i._4),
         Array[String]("L_COMMITDATE","ORDERKEY","LINENUMBER","L_RECEIPTDATE"), cnt))
         case "+OR" =>
         action = "Insert"
         relation = "orders"
         val i = Tuple4(cells(1).toLong,cells(0).toLong,format.parse(cells(4)),cells(5))
         cnt = cnt + 1
         ctx.output(ordersTag, Payload(relation, action, cells(1).toLong.asInstanceOf[Any],
         Array[Any](i._1,i._2,i._3,i._4),
         Array[String]("CUSTKEY","ORDERKEY","O_ORDERDATE","O_ORDERPRIORITY"), cnt))
         case "-OR" =>
         action = "Delete"
         relation = "orders"
         val i = Tuple4(cells(1).toLong,cells(0).toLong,format.parse(cells(4)),cells(5))
         cnt = cnt + 1
         ctx.output(ordersTag, Payload(relation, action, cells(1).toLong.asInstanceOf[Any],
         Array[Any](i._1,i._2,i._3,i._4),
         Array[String]("CUSTKEY","ORDERKEY","O_ORDERDATE","O_ORDERPRIORITY"), cnt))
         case _ =>
         out.collect(Payload("", "", 0, Array(), Array(), 0))
         }
         }).setParallelism(1)
         restDS
      }
      }
