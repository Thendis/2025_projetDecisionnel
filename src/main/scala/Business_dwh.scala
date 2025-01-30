
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.{functions => F};
import org.apache.spark.sql.SaveMode;
import java.util.Properties;

	
object Business_dwh {
  def main(args: Array[String]) {
  val spark = SparkSession
    .builder
    .appName("Business_CEA")
    .master("local")
    .getOrCreate();


  val df_business = readFromQuery(spark,"select 	business.business_id,	is_open,	latitude,	longitude,	review_count,	stars,	address,	state,	city,	name,	postal_code,	coalesce(count(date),0) checkin_count	from 	business left join checkin on checkin.business_id = business.business_id	group by 1,2,3,4,5,6,7,8,9,10,11");
  sendToPsql(df_business, "business_dwh");
  spark.stop();
  }

  /**
   * Retourne un dataframe a partir d'une requête SQL
   * spark = SparkSession initialisé
   * query = Le string de la query demandé. Doit contenir un SELECT
   * nom_base = l'identifiant de l'étudiant ou récupéer la base (Initialisé à an450821)
   * 
   * Return : DataFrame avec le résultat de la requête
  */
  def readFromQuery(spark:SparkSession, query:String, nom_base:String = "an450821" ): org.apache.spark.sql.DataFrame =
    return spark.read.format("jdbc")
    .option("url", "jdbc:postgresql://kafka:5432/"+nom_base)
    .option("driver", "org.postgresql.Driver")
    .option("query", query)
    .option("user", nom_base)
    .option("password", nom_base)
    .load()
  

  /**
   * Retourne un dataframe a partir du nom d'une table
   * spark = SparkSession initialisé
   * nom_table = Le nom de la table
   * nom_base = l'identifiant de l'étudiant ou récupéer la base (Initialisé à an450821)
   * 
   * Return : DataFrame avec le résultat de la requête
  */
  def readFromTable(spark:SparkSession, nom_table:String, nom_base:String = "an450821" ): org.apache.spark.sql.DataFrame =
    return spark.read.format("jdbc")
    .option("url", "jdbc:postgresql://kafka:5432/"+nom_base)
    .option("driver", "org.postgresql.Driver")
    .option("dbtable", nom_table)
    .option("user", nom_base)
    .option("password", nom_base)
    .load()
  
  /**
   * Envoie un dataframe en tant que table sur une base de donnée. (Drop and create de base)
   * df = Le dataframe source
   * nom_table = Le nom de la table
   * nom_base = l'identifiant de l'étudiant ou récupéer la base (Initialisé à an450821)
   * 
   * Return : None
  */
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
