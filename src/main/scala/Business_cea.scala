
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.{functions => F};
import org.apache.spark.sql.SaveMode;
import java.util.Properties;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.IntegerType;
import tools.Tools;

object Business_cea {


  def main(args: Array[String]):Unit= {
  val spark = SparkSession
    .builder
    .appName("Business_CEA")
    .master("local")
    .getOrCreate();
    val tools = new Tools()

  val path = "/home1/em963948/Desktop/datasets/ds_business.json";
  val colsOk: List[String] = List("address",  "business_id",  "city",
    "is_open", "latitude", "longitude", "name", "postal_code", "review_count", "stars", "state", "hours", "categories", "attributes");


  val colNames = colsOk.map(name => F.col(name))  // Convertion en colonne
  var df = spark.read.json(path).select(colNames:_*);

  //Split Hours en heure par jour
    val daysOfWeek = Seq("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")

    // Ajout des colonnes Open/Close pour chaque jour de la semaine
    for (day <- daysOfWeek) {
      df = df
        .withColumn(s"${day}_open", F.lower(F.trim(F.split(F.col("hours").getItem(day), "-").getItem(0).cast(StringType))))  // Extrait du json le jour et le fragmente
        .withColumn(s"${day}_close", F.lower(F.trim(F.split(F.col("hours").getItem(day), "-").getItem(1).cast(StringType))))  // Extrait du json le jour et le fragmente
    }
    df = df.drop("hours");

    // Récupère les 6 premières catégories, trim et lower
    df = df
        .withColumn("categorie0", F.lower(F.trim(F.split(F.col("categories"), ",").getItem(0))).cast(StringType))
        .withColumn("categorie1", F.lower(F.trim(F.split(F.col("categories"), ",").getItem(1))).cast(StringType))
        .withColumn("categorie2", F.lower(F.trim(F.split(F.col("categories"), ",").getItem(2))).cast(StringType))
        .withColumn("categorie3", F.lower(F.trim(F.split(F.col("categories"), ",").getItem(3))).cast(StringType))
        .withColumn("categorie4", F.lower(F.trim(F.split(F.col("categories"), ",").getItem(4))).cast(StringType))
        .withColumn("categorie5", F.lower(F.trim(F.split(F.col("categories"), ",").getItem(5))).cast(StringType));
    
    df = df.drop("categories");

    // Récupère le boolean de vente d'alcohol
    df = df
    .withColumn("alcohol", F.col("attributes").getItem("Alcohol").cast(StringType))
    
    df = df.withColumn("alcohol",
    F.when(F.col("alcohol") === "u'full_bar'", "full_bar")
    .when(F.col("alcohol") === "'full_bar'", "full_bar")
    .when(F.col("alcohol") === "u'beer_and_wine'", "beer_and_wine")
    .when(F.col("alcohol") === "'beer_and_wine'", "beer_and_wine")
    .otherwise(null))
    

    df = df.drop("attributes");

    df.printSchema()
    df.groupBy("alcohol").count().show()
    tools.sendToPsql(df, "business_cea")
    spark.stop();
  }

  
}
