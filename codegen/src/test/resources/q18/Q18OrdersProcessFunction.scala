import scala.math.Ordered.orderingToOrdered
import org.hkust.BasedProcessFunctions.RelationFKCoProcessFunction
import org.hkust.RelationType.Payload
import java.util.Date
class Q18OrdersProcessFunction extends RelationFKCoProcessFunction[Any]("orders",1,Array("CUSTKEY"),Array("ORDERKEY"),false, true) {
override def isValid(value: Payload): Boolean = {
   true
   }
   }
