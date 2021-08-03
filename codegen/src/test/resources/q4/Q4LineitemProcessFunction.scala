import scala.math.Ordered.orderingToOrdered
import org.hkust.BasedProcessFunctions.RelationFKCoProcessFunction
import org.hkust.RelationType.Payload
import java.util.Date
class Q4LineitemProcessFunction extends RelationFKCoProcessFunction[Any]("lineitem",1,Array("ORDERKEY"),Array("ORDERKEY"),true, true) {
override def isValid(value: Payload): Boolean = {
   if(value("L_COMMITDATE").asInstanceOf[java.util.Date]<value("L_RECEIPTDATE").asInstanceOf[java.util.Date]){
   true}else{
   false}
}
}
