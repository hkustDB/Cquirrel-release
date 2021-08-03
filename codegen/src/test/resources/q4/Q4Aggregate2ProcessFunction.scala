import org.hkust.RelationType.Payload
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.hkust.BasedProcessFunctions.AggregateProcessFunction

class Q4Aggregate2ProcessFunction extends AggregateProcessFunction[Any, Integer]("Q4Aggregate2ProcessFunction", Array("O_ORDERPRIORITY"), Array("O_ORDERPRIORITY"), aggregateName = "order_count", deltaOutput = true) {
  override def aggregate(value: Payload): Integer = {
    if (value("DIST_COUNT").asInstanceOf[String].toInt == 1) 1
    if (value("DIST_COUNT").asInstanceOf[String].toInt == 0) -1
    else 0
  }

  override def addition(value1: Integer, value2: Integer): Integer = value1 + value2

  override def subtraction(value1: Integer, value2: Integer): Integer = value1 - value2

  override def initstate(): Unit = {
    val valueDescriptor = TypeInformation.of(new TypeHint[Integer]() {})
    val aliveDescriptor: ValueStateDescriptor[Integer] = new ValueStateDescriptor[Integer]("Q4Aggregate2ProcessFunction" + "Alive", valueDescriptor)
    alive = getRuntimeContext.getState(aliveDescriptor)
  }

  override val init_value: Integer = 0
}
