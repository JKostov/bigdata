import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

// TODO: number of accidents in city in the given month

public class Main {
    public static void main(String[] args) {
        if (args.length < 1) {
            throw new IllegalArgumentException("Csv file path on the hdfs must be passed as argument");
        }

        String sparkMasterUrl = System.getenv("SPARK_MASTER_URL");
        if (sparkMasterUrl == null || sparkMasterUrl.equals("")) {
            throw new IllegalStateException("SPARK_MASTER_URL environment variable must be set.");
        }

        String hdfsUrl = System.getenv("HDFS_URL");
        if (hdfsUrl == null || hdfsUrl.equals("")) {
            throw new IllegalStateException("HDFS_URL environment variable must be set");
        }

        String hdfsPath = args[0];
        String csvFile = hdfsUrl + hdfsPath;

        SparkSession spark = SparkSession.builder().appName("BigData-3").master(sparkMasterUrl).getOrCreate();

        Dataset<Row> dataSet = spark.read().option("header", "true").csv(csvFile);

        ShowTrafficEventTypesCountByCityandTimeSpan(dataSet);

        spark.stop();
        spark.close();
    }

    static void ShowTrafficEventTypesCountByCityandTimeSpan(Dataset<Row> ds) {

        ds.filter(ds.col("Type").equalTo("Accident"))
                .select(functions.unix_timestamp(functions.date_format(ds.col("StartTime(UTC)"), "yyyy-MM-00 00:00:00.0")).alias("Month"), ds.col("City"))
//                .groupBy("City")
//                .agg(functions.count("City"))
                .show()
        ;
    }
}
