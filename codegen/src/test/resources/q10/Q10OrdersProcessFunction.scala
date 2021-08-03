import scala.math.Ordered.orderingToOrdered
import org.hkust.BasedProcessFunctions.RelationFKCoProcessFunction
import org.hkust.RelationType.Payload
import java.util.Date
class Q10OrdersProcessFunction extends RelationFKCoProcessFunction[Any]("orders",1,Array("CUSTKEY"),Array("ORDERKEY"),false, true) {
override def isValid(value: Payload): Boolean = {
   if(value("O_ORDERDATE").asInstanceOf[java.util.Date]>=format.parse("1993-10-01")&&value("O_ORDERDATE").asInstanceOf[java.util.Date]<format.parse("1994-01-01")){
   true}else{
   false}
}
}
