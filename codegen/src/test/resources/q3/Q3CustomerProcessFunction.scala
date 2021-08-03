import scala.math.Ordered.orderingToOrdered
import org.hkust.BasedProcessFunctions.RelationFKProcessFunction
import org.hkust.RelationType.Payload
import java.util.Date
class Q3CustomerProcessFunction extends RelationFKProcessFunction[Any]("customer",Array("CUSTKEY"),Array("CUSTKEY"),false) {
override def isValid(value: Payload): Boolean = {
   if(value("C_MKTSEGMENT").asInstanceOf[String]=="BUILDING"){
   true}else{
   false}
}
}
