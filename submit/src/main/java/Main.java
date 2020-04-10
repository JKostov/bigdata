import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Main {
    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {
//        Thread.sleep(40000);
        if(args.length < 4) {
            throw new IllegalArgumentException("Owner, max_detail, input and output folder must be passed as arguments");
        }

        String sparkMasterUrl = System.getenv("SPARK_MASTER_URL");
        if(StringUtils.isBlank(sparkMasterUrl)) {
            throw new IllegalStateException("SPARK_MASTER_URL environment variable must be set.");
        }

        String hdfsUrl = System.getenv("HDFS_URL");
        if(StringUtils.isBlank(hdfsUrl)) {
            throw new IllegalStateException("HDFS_URL environment variable must be set");
        }

        String owner = args[0];
        int maxDetail = Integer.parseInt(args[1]);
        String inputArg = args[2];
        if(!inputArg.endsWith("/")) { inputArg += "/"; }
        String csvFile = hdfsUrl + inputArg + owner + ".csv";
        String outputArg = args[3];
        if(!outputArg.endsWith("/")) { outputArg += "/"; }
        String outputPath = outputArg + owner;

        SparkConf sparkConf = new SparkConf().setAppName("BDE-SensorDemo").setMaster(sparkMasterUrl);
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(sparkContext);
        FileSystem hdfs = FileSystem.get(new URI(hdfsUrl), sparkContext.hadoopConfiguration());

        System.out.println("TESTING------------------------");
        System.out.println(sparkMasterUrl);
        System.out.println(hdfsUrl);
        System.out.println(owner);
        System.out.println(maxDetail);
        System.out.println(csvFile);
        System.out.println(outputPath);

        hdfs.close();
        sparkContext.close();
        sparkContext.stop();
    }
}
