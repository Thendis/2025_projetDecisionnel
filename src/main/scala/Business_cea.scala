
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.{functions => F};
import org.apache.spark.sql.SaveMode;
import java.util.Properties;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.IntegerType;
	
object Business_cea {

  // Fonction pour convertir "heure:minutes" en secondes
def timeToMinutes(time: String): Int = {
  if(time == null)
    return -1

  val parts = time.split(":");
  val hours = parts(0).toInt;
  val minutes = parts(1).toInt;
  return hours * 60 + minutes ;
}

// Enregistrer la fonction comme UDF (User Define Function)
val timeToSecondsUDF = F.udf(timeToMinutes _)

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

  def main(args: Array[String]) {
  val spark = SparkSession
    .builder
    .appName("Business_CEA")
    .master("local")
    .getOrCreate();

  val path = "/home1/em963948/Desktop/datasets/ds_business.json";
  val colsOk: List[String] = List("address",  "business_id",  "city",
    "is_open", "latitude", "longitude", "name", "postal_code", "review_count", "stars", "state", "hours");


  val colNames = colsOk.map(name => F.col(name))  // Convertion en colonne
  var df = spark.read.json(path).select(colNames:_*);

  //Split Hours en heure par jour
    val daysOfWeek = Seq("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")

    // Ajout des colonnes Open/Close pour chaque jour de la semaine
    for (day <- daysOfWeek) {
      df = df
        .withColumn(s"${day}_open", F.split(F.col("hours").getItem(day), "-").getItem(0).cast(StringType))  // Extrait du json le jour et le fragmente
        .withColumn(s"${day}_close", F.split(F.col("hours").getItem(day), "-").getItem(1).cast(StringType))  // Extrait du json le jour et le fragmente
    }
    df = df.drop("hours");
    //Passe du format heure:minute à un nombre de minutes depuis jusqu'à l'évènnement
    // Liste des colonnes à convertir
    val columnsToConvert = Seq(
      "monday_open", "monday_close", "tuesday_open", "tuesday_close",
      "wednesday_open", "wednesday_close", "thursday_open", "thursday_close",
      "friday_open", "friday_close", "saturday_open", "saturday_close",
      "sunday_open", "sunday_close"
    )

    // Appliquer la UDF à chaque colonne
    df = columnsToConvert.foldLeft(df) { (accDF, colName) =>
      accDF.withColumn(colName, timeToSecondsUDF(F.col(colName)).cast(IntegerType))
    }

    df.printSchema();
    df.show(5);
    sendToPsql(df, "business")
  
    spark.stop();
  }

  
}
