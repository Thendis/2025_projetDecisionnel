import java.sql.Time;
import org.apache.spark.sql.{SaveMode, SparkSession};
import org.apache.spark.sql.{functions => F};
import org.apache.spark.sql.types.StringType;
import tools.Tools;
import os.list



object LABO{

    def main(args: Array[String]):Unit={
        val spark = SparkSession
        .builder
        .appName("load_zone")
        .master("local[*]")
        .getOrCreate()
        
        val tools = new Tools()

        var df = spark
        .read
        .option("header", "true")
        .csv("/home1/em963948/Desktop/datasets/country_us_boundaries/cb_2018_us_county_500k.csv")

        tools.executeSqlFromFile(dir = "create_script_sql", file = "create_usa_geo.sql")
        df.printSchema();
        tools.sendToPsql(df, nom_table = "usa_geo_ref");
        tools.executeSqlFromFile( file = "update_usa_geo_ref.sql")
        spark.stop();
    }

} 

