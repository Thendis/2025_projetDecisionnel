
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.{functions => F};
import org.apache.spark.sql.SaveMode;
import java.util.Properties;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.IntegerType;
import tools.Tools;

object Business_planning_axe {
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


    var df = tools.readFromQuery(spark, query="select * from business_cea")

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

    tools.sendToPsql(df, "business_planning_axe")
    spark.stop();

    val path: os.Path = os.pwd / "update_scripts_sql" / "update_business_axe.sql";
    val file = os.read(path);
    tools.execute(query=file);
} 
}
