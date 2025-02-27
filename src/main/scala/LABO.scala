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

        //Load states
        var df = spark
        .read
        .option("header", "true")
        .csv("/home1/em963948/Desktop/datasets/country_us_boundaries/cb_2023_us_county.csv")

        tools.executeSqlFromFile(dir = "create_script_sql", file = "create_usa_geo.sql")
        df.printSchema();
        tools.sendToPsql(df, nom_table = "usa_geo_ref");
        

        //Load roads
        df = spark
        .read
        .option("header", "true")
        .csv("/home1/em963948/Desktop/datasets/NTAD_North_American_Roads_-6941702301048783378/american_roads.csv")

        df.printSchema();
        tools.sendToPsql(df.select("*").filter("speedlim > 110"), nom_table = "usa_road_geo_ref");

        tools.executeSqlFromFile( file = "update_usa_geo_ref.sql")
        tools.executeSqlFromFile( dir="create_script_sql", file = "create_geo_datamart.sql")
        spark.stop();
    }

} 

