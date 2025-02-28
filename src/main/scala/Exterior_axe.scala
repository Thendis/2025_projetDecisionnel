import java.sql.Time;
import org.apache.spark.sql.{SaveMode, SparkSession};
import org.apache.spark.sql.{functions => F};
import org.apache.spark.sql.types.StringType;
import tools.Tools;
import os.list



object Exterior_axe{

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
        .csv("dataset/cb_2023_us_county.csv")

        tools.executeSqlFromFile(dir = "create_script_sql", file = "create_usa_geo.sql")
        df.printSchema();
        tools.sendToPsql(df, nom_table = "usa_geo_axe");
        

        //Load roads
        df = spark
        .read
        .option("header", "true")
        .csv("dataset/american_roads.csv")

        df.printSchema();
        tools.sendToPsql(df.select("*").filter("speedlim > 110"), nom_table = "usa_road_geo_axe");


        //Load les scripts
        tools.executeSqlFromFile( file = "update_usa_geo_axe.sql")
        tools.executeSqlFromFile( dir="create_script_sql", file = "create_geo_datamart.sql")
        tools.executeSqlFromFile( dir="create_script_sql", file = "create_alcohol_vs_no_alcohol_fat.sql")
        spark.stop();
    }

} 

