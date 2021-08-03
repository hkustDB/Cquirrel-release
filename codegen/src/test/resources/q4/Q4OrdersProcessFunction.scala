import scala.math.Ordered.orderingToOrdered
import org.hkust.BasedProcessFunctions.RelationFKProcessFunction
import org.hkust.RelationType.Payload
import java.util.Date
class Q4OrdersProcessFunction extends RelationFKProcessFunction[Any]("orders",Array("CUSTKEY"),Array("ORDERKEY"),false) {
override def isValid(value: Payload): Boolean = {
   if(value("O_ORDERDATE").asInstanceOf[java.util.Date]>=format.parse("1993-07-01")&&value("O_ORDERDATE").asInstanceOf[java.util.Date]<format.parse("1993-10-01")){
   true}else{
   false}
}
}
