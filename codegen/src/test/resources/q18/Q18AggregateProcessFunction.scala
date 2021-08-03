import org.hkust.RelationType.Payload
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.hkust.BasedProcessFunctions.AggregateProcessFunction
class Q18AggregateProcessFunction extends AggregateProcessFunction[Any, Double]("Q18AggregateProcessFunction", Array("ORDERKEY"), Array("C_NAME","CUSTKEY","ORDERKEY","O_ORDERDATE","O_TOTALPRICE"), aggregateName = "AGGREGATE", deltaOutput = true) {
   override def aggregate(value: Payload): Double = {
      value("L_QUANTITY").asInstanceOf[Double]
   }
   override def addition(value1: Double, value2: Double): Double = value1 + value2
   override def subtraction(value1: Double, value2: Double): Double = value1 - value2
   override def isOutputValid(value: Payload): Boolean = {if(value("AGGREGATE").asInstanceOf[String].toDouble>300){
   true}else{
   false}
}
override def initstate(): Unit = {
   val valueDescriptor = TypeInformation.of(new TypeHint[Double](){})
   val aliveDescriptor : ValueStateDescriptor[Double] = new ValueStateDescriptor[Double]("Q18AggregateProcessFunction"+"Alive", valueDescriptor)
   alive = getRuntimeContext.getState(aliveDescriptor)
   }
      override val init_value: Double = 0.0
      }
