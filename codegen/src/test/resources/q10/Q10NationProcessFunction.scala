import scala.math.Ordered.orderingToOrdered
import org.hkust.BasedProcessFunctions.RelationFKProcessFunction
import org.hkust.RelationType.Payload
import java.util.Date
class Q10NationProcessFunction extends RelationFKProcessFunction[Any]("nation",Array("NATIONKEY"),Array("NATIONKEY"),false) {
override def isValid(value: Payload): Boolean = {
   true
   }
   }
