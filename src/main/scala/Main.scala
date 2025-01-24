
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.{functions => F};
import org.apache.spark.sql.SaveMode;
import java.util.Properties

object Main extends App {
  private val spark = SparkSession
    .builder
    .appName("Business_CEA")
    .master("local")
    .getOrCreate();

  val path = "/home/ravenclaw/Documents/datasets/yelp_ds_business.json";
  val colsOk: List[String] = List("address",  "business_id",  "city",
    "is_open", "latitude", "longitude", "name", "postal_code", "review_count", "stars", "state");
  val colsNotOk: List[String] = List("attributes", "categories", "hours");


  val colNames = colsOk.map(name => F.col(name))  // Convertion en colonne
  val df = spark.read.json(path).select(colNames:_*);


  df.printSchema();

  // Paramètres de la connexion BD
  Class.forName("org.postgresql.Driver")
  val url = "jdbc:postgresql://kafka:5432/em963948"
  val connectionProperties = new Properties();
  connectionProperties.setProperty("driver", "org.postgresql.Driver");
  connectionProperties.setProperty("user", "em963948");
  connectionProperties.setProperty("password","em963948");

  // Enregistrement du DataFrame users dans la table "user"
  df.write.mode(SaveMode.Overwrite).jdbc(url, "business_cea", connectionProperties);

  spark.stop();
}
