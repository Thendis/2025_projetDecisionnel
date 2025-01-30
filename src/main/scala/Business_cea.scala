
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.{functions => F};
import org.apache.spark.sql.SaveMode;
import java.util.Properties;

	
object Business_cea {
  def main(args: Array[String]) {
  val spark = SparkSession
    .builder
    .appName("Business_CEA")
    .master("local")
    .getOrCreate();

  val path = "/home1/em963948/Desktop/datasets/ds_business.json";
  val colsOk: List[String] = List("address",  "business_id",  "city",
    "is_open", "latitude", "longitude", "name", "postal_code", "review_count", "stars", "state");
  val colsNotOk: List[String] = List("attributes", "categories", "hours");


  val colNames = colsOk.map(name => F.col(name))  // Convertion en colonne
  val df = spark.read.json(path).select(colNames:_*);


  df.printSchema();

  sendToPsql(df, "business")
  
  spark.stop();
  }

  def sendToPsql(df: org.apache.spark.sql.DataFrame, nom_table:String, nom_base: String="an450821"){
    // Paramètres de la connexion BD

    Class.forName("org.postgresql.Driver")
    val url = "jdbc:postgresql://kafka:5432/"+nom_base
    val connectionProperties = new Properties();
    connectionProperties.setProperty("driver", "org.postgresql.Driver");
    connectionProperties.setProperty("user", nom_base);
    connectionProperties.setProperty("password",nom_base);

    // Enregistrement du DataFrame users dans la table "user"
    df.write.mode(SaveMode.Overwrite).jdbc(url, nom_table, connectionProperties);
    }
}
