import scala.math.Ordered.orderingToOrdered
import org.hkust.BasedProcessFunctions.RelationFKCoProcessFunction
import org.hkust.RelationType.Payload
import java.util.Date
class Q18lineitemProcessFunction extends RelationFKCoProcessFunction[Any]("lineitem",1,Array("ORDERKEY"),Array("ORDERKEY"),true, true) {
override def isValid(value: Payload): Boolean = {
   true
   }
   }
