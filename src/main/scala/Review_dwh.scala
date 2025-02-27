
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.{functions => F};
import org.apache.spark.sql.SaveMode;
import java.util.Properties;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.IntegerType;
import tools.Tools;

object Review_dwh{


  def main(args: Array[String]):Unit= {

    val tools = new Tools();
    val spark = SparkSession
    .builder
    .appName("Business_CEA")
    .master("local")
    .getOrCreate();

    
    var df = tools.readFromQuery(spark = spark, query = "select review.*, coalesce(elite.year,9999) from yelp.review left join yelp.elite on review.user_id = elite.user_id where spark_partition = 0", nom_base = "tpid2020", user = "tpid", password="tpid",  host="stendhal");
    
    df.printSchema()
    tools.sendToPsql(df, "review_dwh")
    val rg = new Range(1,100,1)
    for (i <- rg){
      var df = tools.readFromQuery(spark = spark, query = "select review.*, coalesce(elite.year,9999) from yelp.review left join yelp.elite on review.user_id = elite.user_id where spark_partition = "+i, nom_base = "tpid2020", user = "tpid", password="tpid",  host="stendhal");
    
      df.printSchema()
      tools.sendToPsql(df, "review_dwh", rewrite = false)
    }
    
  
    spark.stop();
  }

  
}
