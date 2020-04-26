import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

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

//        ShowNumberOfAccidentsPerCity(dataSet);

        Dataset<Row> test = dataSet.filter(dataSet.col("Type").equalTo("Accident").and(dataSet.col("City").equalTo("Los Angeles")))
                .select(functions.unix_timestamp(functions.date_format(dataSet.col("StartTime(UTC)"), "yyyy-MM-01"), "yyyy-MM-dd").alias("Month"), dataSet.col("City"))
                .groupBy("City", "Month")
                .agg(functions.count("City").cast(DataTypes.DoubleType).alias("NumberOfAccidentsPerMonth"));

        Dataset<Row> test2 = test.select(test.col("Month"), test.col("NumberOfAccidentsPerMonth"));

        test2.show();

        List<StructField> fields = new ArrayList<StructField>();
        fields.add(DataTypes.createStructField("Month", DataTypes.DoubleType, true));
        fields.add(DataTypes.createStructField("NumberOfAccidentsPerMonth", new VectorUDT(), true));
        StructType schema = DataTypes.createStructType(fields);

        JavaRDD<Tuple2<Double, Vector>> javaRddTuple = test2.rdd().toJavaRDD().map(f -> {
            Object m = f.get(0);
            long l = (long)m;

            return new Tuple2<>((double) l, Vectors.dense((double)f.get(1)));
        });

        JavaRDD<Row> javaRddRow = javaRddTuple.map(tup -> {
            Double doub = tup._1();
            Vector vect = tup._2();
            return RowFactory.create(doub, vect);
        });

        Dataset<Row> converted = spark.createDataFrame(javaRddRow, schema);

        converted.show(10000);

        LinearRegression lr = new LinearRegression()
                .setMaxIter(10)
                .setRegParam(0.3)
                .setElasticNetParam(0.8);

        LinearRegressionModel lrModel = lr.setLabelCol("Month").setFeaturesCol("NumberOfAccidentsPerMonth").fit(converted);

        Date a = new Date(2017, Calendar.APRIL, 1);
        double asd = a.getTime() / 1000.0;

        System.out.println("PREDICTION---------------------------------------------------");
        System.out.println(asd);
        System.out.println(lrModel.predict(Vectors.dense(asd)));
        System.out.println("PREDICTION---------------------------------------------------");

        spark.stop();
        spark.close();
    }

    static void ShowNumberOfAccidentsPerCity(Dataset<Row> ds) {

        ds.filter(ds.col("Type").equalTo("Accident"))
                .select(functions.unix_timestamp(functions.date_format(ds.col("StartTime(UTC)"), "yyyy-MM-01"), "yyyy-MM-dd").alias("Month"), ds.col("City"))
                .groupBy("City", "Month")
                .agg(functions.count("City").alias("NumberOfAccidentsPerMonth"))
                .show()
        ;
    }
}
