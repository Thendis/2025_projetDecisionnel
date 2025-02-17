
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.{functions => F};
import org.apache.spark.sql.SaveMode;
import java.util.Properties;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.IntegerType;
import tools.Tools;

object Business_dwh {
    // Fonction pour convertir "heure:minutes" en secondes
def timeToMinutes(time: String): Int = {
  if(time == null)
    return 0

  val parts = time.split(":");
  val hours = parts(0).toInt;
  val minutes = parts(1).toInt;
  return hours * 60 + minutes ;
}
// Enregistrer la fonction comme UDF (User Define Function)
val timeToSecondsUDF = F.udf(timeToMinutes _)

def main(args: Array[String]):Unit= {
  val spark = SparkSession
    .builder
    .appName("Business_AXE")
    .master("local")
    .getOrCreate();
    val tools = new Tools()


  var df = tools.readFromQuery(spark,"SELECT business_cea.business_id, is_open, latitude, longitude, review_count, stars, address, state, city, name, postal_code, coalesce(count(date),  0) checkin_count, alcohol, monday_open, monday_close, tuesday_open, tuesday_close, wednesday_open, wednesday_close, thursday_open, thursday_close, friday_open, friday_close, saturday_open, saturday_close, sunday_open, sunday_close, categorie0, categorie1, categorie2, categorie3, categorie4, categorie5 FROM business_cea LEFT JOIN checkin ON checkin.business_id = business_cea.business_id GROUP BY business_cea.business_id, is_open, latitude, longitude, review_count, stars, address, state, city, name, postal_code, alcohol, monday_open, monday_close, tuesday_open, tuesday_close, wednesday_open, wednesday_close, thursday_open, thursday_close, friday_open, friday_close, saturday_open, saturday_close, sunday_open, sunday_close, categorie0, categorie1, categorie2, categorie3, categorie4, categorie5");

    //Passe du format heure:minute à un nombre de minutes jusqu'à l'évènnement
    // Liste des colonnes à convertir
    val columnsToConvert = Seq(
      "monday_open", "monday_close", "tuesday_open", "tuesday_close",
      "wednesday_open", "wednesday_close", "thursday_open", "thursday_close",
      "friday_open", "friday_close", "saturday_open", "saturday_close",
      "sunday_open", "sunday_close"
    )

    // Appliquer la UDF à chaque colonne
    df = columnsToConvert.foldLeft(df) { (accDF, colName) =>
      accDF.withColumn(colName+"_minutes", timeToSecondsUDF(F.col(colName)).cast(IntegerType))
    }

    tools.sendToPsql(df, "business_dwh")
    spark.stop();

    tools.executeSqlFromFile(file="update_business_dwh.sql");
  } 
}
