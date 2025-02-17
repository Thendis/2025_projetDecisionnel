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
        .appName("Business_AXE")
        .master("local")
        .getOrCreate();
        
        val tools = new Tools()

        var df = spark.read
        .option("delimiter", ";")
        .option("header", "true")
        .csv("/home1/em963948/Desktop/datasets/us-state-boundaries.csv");


        df.printSchema();
        df.show(5)
        tools.sendToPsql(df, nom_table = "usa_state_geometry_ref");
        spark.stop();
    }

} 

