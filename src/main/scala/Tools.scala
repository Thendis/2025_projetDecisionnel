package tools;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SaveMode;
import java.util.Properties;
import java.sql.{Connection, DriverManager, ResultSet}

class Tools {
   
  /**
    * Retourne un dataframe a partir d'une requête SQL
    * @param spark SparkSession initialisé
    * @param query Le string de la query demandé. Doit contenir un SELECT
    * @param nom_base l'identifiant de l'étudiant ou récupéer la base (Initialisé à em963948)
    * @param user
    * @param password
    * @param host
    * @param port
    * @return DataFrame avec le résultat de la requête
    */
  def readFromQuery(spark:SparkSession, query:String, nom_base:String = "em963948", user:String = "em963948", password:String = "em963948", host:String = "kafka", port:Integer=5432 ): org.apache.spark.sql.DataFrame =
    return spark.read.format("jdbc")
    .option("url", "jdbc:postgresql://"+host+":"+port+"/"+nom_base)
    .option("driver", "org.postgresql.Driver")
    .option("query", query)
    .option("user", user)
    .option("password", password)
    .load()
  
    /**
      * Execute une requête sur la base 
      * @param query
      * @param nom_base
      * @param user
      * @param password
      * @param host
      * @param port
      */
  def execute(query:String, nom_base:String = "em963948", user:String = "em963948", password:String = "em963948", host:String = "kafka", port:Integer=5432 ): Unit={
    val con_str = "jdbc:postgresql://"+host+":"+port+"/"+nom_base+"?user="+user+"&password="+password
    val conn = DriverManager.getConnection(con_str)
    try {
      val stm = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

      val rs = stm.execute(query)

  } finally {
      conn.close()
    }
  }

  /**
    * Retourne un dataframe a partir du nom d'une table
    * @param spark SparkSession initialisé
    * @param query Le string de la query demandé. Doit contenir un SELECT
    * @param nom_base l'identifiant de l'étudiant ou récupéer la base (Initialisé à em963948)
    * @param user
    * @param password
    * @param host
    * @param port
    * @return DataFrame avec le résultat de la requête
    */
  def readFromTable(spark:SparkSession, nom_table:String, nom_base:String = "em963948", user:String = "em963948", password:String = "em963948", host:String = "kafka", port:Integer=5432 ): org.apache.spark.sql.DataFrame =
    return spark.read.format("jdbc")
    .option("url", "jdbc:postgresql://"+host+":"+port+"/"+nom_base)
    .option("driver", "org.postgresql.Driver")
    .option("dbtable", nom_table)
    .option("user", user)
    .option("password", password)
    .load()
  
  
  /**
    * 
    *
    * @param df Le dataframe a envoyer en base
    * @param nom_table
    * @param nom_base l'identifiant de l'étudiant ou récupéer la base (Initialisé à em963948)
    */
  def sendToPsql(df: org.apache.spark.sql.DataFrame, nom_table:String, nom_base: String="em963948", rewrite: Boolean = true): Unit={
    // Paramètres de la connexion BD
    print("===> Sending Dataframe with sendToPsql. See next")
    df.printSchema();
    df.show(5);

    Class.forName("org.postgresql.Driver")
    val url = "jdbc:postgresql://kafka:5432/"+nom_base
    val connectionProperties = new Properties();
    connectionProperties.setProperty("driver", "org.postgresql.Driver");
    connectionProperties.setProperty("user", nom_base);
    connectionProperties.setProperty("password",nom_base);
  

    // Enregistrement du DataFrame users dans la table "user"
    if(rewrite){
      df.write.mode(SaveMode.Overwrite).option("batchsize", 1000).jdbc(url, nom_table, connectionProperties);
    } else{
      df.write.mode(SaveMode.Append).option("batchsize", 1000).jdbc(url, nom_table, connectionProperties);
    } 
  } 
  
  def executeSqlFromFile(dir:String="update_scripts_sql", file:String){
    val path: os.Path = os.pwd / "SQL" / dir / file;
    val str_file = os.read(path);
    execute(query=str_file)
  }
}
