import scala.math.Ordered.orderingToOrdered
import org.hkust.BasedProcessFunctions.RelationFKCoProcessFunction
import org.hkust.RelationType.Payload
import java.util.Date
class Q10CustomerProcessFunction extends RelationFKCoProcessFunction[Any]("customer",1,Array("NATIONKEY"),Array("CUSTKEY"),false, true) {
override def isValid(value: Payload): Boolean = {
   true
   }
   }
