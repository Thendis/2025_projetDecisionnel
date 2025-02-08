
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.{functions => F};
import org.apache.spark.sql.SaveMode;
import java.util.Properties;
import tools.Tools;
	
object Business_dwh {
  def main(args: Array[String]): Unit = {
  val spark = SparkSession
    .builder
    .appName("Business_DWH")
    .master("local")
    .getOrCreate();

  val tools = new Tools();

  val df = tools.readFromQuery(spark,"select 	business_cea.business_id,	is_open,	latitude,	longitude,	review_count,	stars,	address,	state,	city,	name,	postal_code,	coalesce(count(date),0) checkin_count	from 	business_cea left join checkin on checkin.business_id = business_cea.business_id	group by 1,2,3,4,5,6,7,8,9,10,11");

  tools.sendToPsql(df, "business_dwh");
  spark.stop();
  }

  
}
