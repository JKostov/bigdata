import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

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

        SparkSession spark = SparkSession.builder().appName("BigData-1").master(sparkMasterUrl).getOrCreate();

//        FileSystem hdfs = FileSystem.get(new URI(hdfsUrl), spark.conf());

        System.out.println("TESTING------------------------");
        System.out.println(sparkMasterUrl);
        System.out.println(hdfsUrl);
        System.out.println(csvFile);

        Dataset<Row> dataSet = spark.read().option("header", "true").csv(csvFile);

        ShowTrafficEventTypesCountByCityAndTimeSpan(dataSet);

//        hdfs.close();
        spark.stop();
        spark.close();
    }

    static void ShowTrafficEventTypesCountByCityAndTimeSpan(Dataset<Row> ds) {
        String city = "Salt Lake City";
        String startTime = "2017-00-00 00:00:00";
        String endTime = "2018-00-00 00:00:00";
        int greaterThan = 500;

        ds.filter(
                ds.col("Source").equalTo("T")
                        .and(ds.col("City").equalTo(city))
                        .and(
                                ds.col("StartTime(UTC)").gt(functions.lit(startTime))
                                .and(ds.col("EndTime(UTC)")).lt(functions.lit(endTime))
                        )
        )
                .groupBy("Type")
                .agg(functions.count(ds.col("Type")))
        .filter(functions.count(ds.col("Type").gt(greaterThan)))
        .show()
        ;
    }
}
