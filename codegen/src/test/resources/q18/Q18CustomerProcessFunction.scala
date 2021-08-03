import scala.math.Ordered.orderingToOrdered
import org.hkust.BasedProcessFunctions.RelationFKProcessFunction
import org.hkust.RelationType.Payload
import java.util.Date
class Q18CustomerProcessFunction extends RelationFKProcessFunction[Any]("customer",Array("CUSTKEY"),Array("CUSTKEY"),false) {
override def isValid(value: Payload): Boolean = {
   true
   }
   }
