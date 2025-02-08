import java.sql.Time;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types._;
import org.apache.spark.sql.{SaveMode, SparkSession};
import org.apache.spark.sql.functions._;
import scala.io.Source.fromFile;

object LABO{

    def main(args: Array[String]):Unit={

        val path: os.Path = os.pwd / "update_scripts_sql" / "update_business_axe.sql";
        val file = os.read(path);
        print(file)
    }

} 

