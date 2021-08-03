import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.hkust.RelationType.Payload
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector
object Job {
   val lineitemTag: OutputTag[Payload] = OutputTag[Payload]("lineitem")
   val ordersTag: OutputTag[Payload] = OutputTag[Payload]("orders")
   val customerTag: OutputTag[Payload] = OutputTag[Payload]("customer")
   val nationTag: OutputTag[Payload] = OutputTag[Payload]("nation")
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
      val nation : DataStream[Payload] = inputStream.getSideOutput(nationTag)
      val customer : DataStream[Payload] = inputStream.getSideOutput(customerTag)
      val nationS = nation.keyBy(i => i._3)
      .process(new Q10NationProcessFunction())
      val customerS = nationS.connect(customer)
      .keyBy(i => i._3, i => i._3)
      .process(new Q10CustomerProcessFunction())
      val ordersS = customerS.connect(orders)
      .keyBy(i => i._3, i => i._3)
      .process(new Q10OrdersProcessFunction())
      val lineitemS = ordersS.connect(lineitem)
      .keyBy(i => i._3, i => i._3)
      .process(new Q10LineitemProcessFunction())
      val result = lineitemS.keyBy(i => i._3)
      .process(new Q10AggregateProcessFunction())
      .map(x => (x._4.mkString(", "), x._5.mkString(", "), x._6))
      .writeAsText(outputpath,FileSystem.WriteMode.OVERWRITE)
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
         val i = Tuple6(cells(3).toInt,cells(0).toLong,cells(5).toDouble,cells(8),cells(15),cells(6).toDouble)
         cnt = cnt + 1
         ctx.output(lineitemTag, Payload(relation, action, cells(0).toLong.asInstanceOf[Any],
         Array[Any](i._1,i._2,i._3,i._4,i._5,i._6),
         Array[String]("LINENUMBER","ORDERKEY","L_EXTENDEDPRICE","L_RETURNFLAG","L_COMMENT","L_DISCOUNT"), cnt))
         case "-LI" =>
         action = "Delete"
         relation = "lineitem"
         val i = Tuple6(cells(3).toInt,cells(0).toLong,cells(5).toDouble,cells(8),cells(15),cells(6).toDouble)
         cnt = cnt + 1
         ctx.output(lineitemTag, Payload(relation, action, cells(0).toLong.asInstanceOf[Any],
         Array[Any](i._1,i._2,i._3,i._4,i._5,i._6),
         Array[String]("LINENUMBER","ORDERKEY","L_EXTENDEDPRICE","L_RETURNFLAG","L_COMMENT","L_DISCOUNT"), cnt))
         case "+OR" =>
         action = "Insert"
         relation = "orders"
         val i = Tuple4(cells(1).toLong,cells(0).toLong,format.parse(cells(4)),cells(8))
         cnt = cnt + 1
         ctx.output(ordersTag, Payload(relation, action, cells(1).toLong.asInstanceOf[Any],
         Array[Any](i._1,i._2,i._3,i._4),
         Array[String]("CUSTKEY","ORDERKEY","O_ORDERDATE","O_COMMENT"), cnt))
         case "-OR" =>
         action = "Delete"
         relation = "orders"
         val i = Tuple4(cells(1).toLong,cells(0).toLong,format.parse(cells(4)),cells(8))
         cnt = cnt + 1
         ctx.output(ordersTag, Payload(relation, action, cells(1).toLong.asInstanceOf[Any],
         Array[Any](i._1,i._2,i._3,i._4),
         Array[String]("CUSTKEY","ORDERKEY","O_ORDERDATE","O_COMMENT"), cnt))
         case "+CU" =>
         action = "Insert"
         relation = "customer"
         val i = Tuple7(cells(0).toLong,cells(3).toLong,cells(1),cells(5).toDouble,cells(4),cells(2),cells(7))
         cnt = cnt + 1
         ctx.output(customerTag, Payload(relation, action, cells(3).toLong.asInstanceOf[Any],
         Array[Any](i._1,i._2,i._3,i._4,i._5,i._6,i._7),
         Array[String]("CUSTKEY","NATIONKEY","C_NAME","C_ACCTBAL","C_PHONE","C_ADDRESS","C_COMMENT"), cnt))
         case "-CU" =>
         action = "Delete"
         relation = "customer"
         val i = Tuple7(cells(0).toLong,cells(3).toLong,cells(1),cells(5).toDouble,cells(4),cells(2),cells(7))
         cnt = cnt + 1
         ctx.output(customerTag, Payload(relation, action, cells(3).toLong.asInstanceOf[Any],
         Array[Any](i._1,i._2,i._3,i._4,i._5,i._6,i._7),
         Array[String]("CUSTKEY","NATIONKEY","C_NAME","C_ACCTBAL","C_PHONE","C_ADDRESS","C_COMMENT"), cnt))
         case "+NA" =>
         action = "Insert"
         relation = "nation"
         val i = Tuple3(cells(0).toLong,cells(1),cells(3))
         cnt = cnt + 1
         ctx.output(nationTag, Payload(relation, action, cells(0).toLong.asInstanceOf[Any],
         Array[Any](i._1,i._2,i._3),
         Array[String]("NATIONKEY","N_NAME","N_COMMENT"), cnt))
         case "-NA" =>
         action = "Delete"
         relation = "nation"
         val i = Tuple3(cells(0).toLong,cells(1),cells(3))
         cnt = cnt + 1
         ctx.output(nationTag, Payload(relation, action, cells(0).toLong.asInstanceOf[Any],
         Array[Any](i._1,i._2,i._3),
         Array[String]("NATIONKEY","N_NAME","N_COMMENT"), cnt))
         case _ =>
         out.collect(Payload("", "", 0, Array(), Array(), 0))
         }
         }).setParallelism(1)
         restDS
      }
      }
