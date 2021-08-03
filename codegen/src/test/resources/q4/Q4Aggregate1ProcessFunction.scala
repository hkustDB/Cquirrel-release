import org.hkust.RelationType.Payload
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.hkust.BasedProcessFunctions.AggregateProcessFunction
class Q4Aggregate1ProcessFunction extends AggregateProcessFunction[Any, Integer]("Q4Aggregate1ProcessFunction", Array("ORDERKEY"), Array("O_ORDERPRIORITY"), aggregateName = "DIST_COUNT", deltaOutput = true) {
   override def aggregate(value: Payload): Integer = {
      1
   }
   override def addition(value1: Integer, value2: Integer): Integer = value1 + value2
   override def subtraction(value1: Integer, value2: Integer): Integer = value1 - value2
   override def initstate(): Unit = {
      val valueDescriptor = TypeInformation.of(new TypeHint[Integer](){})
      val aliveDescriptor : ValueStateDescriptor[Integer] = new ValueStateDescriptor[Integer]("Q4Aggregate1ProcessFunction"+"Alive", valueDescriptor)
      alive = getRuntimeContext.getState(aliveDescriptor)
      }
         override val init_value: Integer = 0
         }
