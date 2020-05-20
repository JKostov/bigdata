import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.expressions.UserDefinedFunction;
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

        // .config("spark.executor.memory", "10g")
        SparkSession spark = SparkSession.builder().appName("BigData-3").master(sparkMasterUrl).getOrCreate();

        Dataset<Row> dataSet = spark.read().option("header", "true").csv(csvFile);

        // EventId=0, Source=1, Type=2, Severity=3, TMC=4, Description=5, StartTime(UTC)=6, EndTime(UTC)=7, TimeZone=8
        // LocationLat=9, LocationLng=10, Distance(mi)=11, AirportCode=12, Number=13, Street=14, Side=15, City=16
        // County=17, State=18, ZipCode=19
        Dataset<Row> traffic = dataSet.filter(dataSet.col("Source").equalTo("T"));
        Dataset<Row> weather = dataSet.filter(dataSet.col("Source").equalTo("W")).select(
                dataSet.col("EventId").as("W-EventId"),
                dataSet.col("Source").as("W-Source"),
                dataSet.col("Type").as("W-Type"),
                dataSet.col("Severity").as("W-Severity"),
                dataSet.col("TMC").as("W-TMC"),
                dataSet.col("Description").as("W-Description"),
                dataSet.col("StartTime(UTC)").as("W-StartTime(UTC)"),
                dataSet.col("EndTime(UTC)").as("W-EndTime(UTC)"),
                dataSet.col("LocationLat").as("W-LocationLat"),
                dataSet.col("LocationLng").as("W-LocationLng"),
                dataSet.col("Street").as("W-Street"),
                dataSet.col("City").as("W-City")
        );


        UDF4<String, String, String, String, Double> udfConvert = Main::distance;
        UserDefinedFunction getDistance = functions.udf(udfConvert, DataTypes.DoubleType);

        Dataset<Row> groupedData = traffic.join(weather,
                traffic.col("City").equalTo(weather.col("W-City"))
                        .and(getDistance.apply(traffic.col("LocationLat"), weather.col("W-LocationLat"), traffic.col("LocationLng"), weather.col("W-LocationLng")).leq(5000.0))
                        .and(traffic.col("StartTime(UTC)").leq(weather.col("W-StartTime(UTC)")).and(traffic.col("EndTime(UTC)").geq(weather.col("W-EndTime(UTC)")))
                                .or(weather.col("W-StartTime(UTC)").leq(traffic.col("StartTime(UTC)")).and(weather.col("W-EndTime(UTC)").geq(traffic.col("EndTime(UTC)")))))
        )
//                .select(traffic.col("EventId"), weather.col("W-EventId"), traffic.col("LocationLat"), weather.col("W-LocationLat"), traffic.col("LocationLng"), weather.col("W-LocationLng"),
//                        traffic.col("Street"), weather.col("W-Street"), traffic.col("StartTime(UTC)"), weather.col("W-StartTime(UTC)"), traffic.col("EndTime(UTC)"), weather.col("W-EndTime(UTC)"),
//                        traffic.col("Type"), weather.col("W-Type"), traffic.col("Severity"), weather.col("W-Severity")
//                );
                .select(traffic.col("EventId"), weather.col("W-EventId"), traffic.col("Type"), weather.col("W-Type"), traffic.col("Severity"), weather.col("W-Severity")
                );

        // TODO: 1. Create model that contains eventIds and numbers for the type and severity - convert type and severity to int
        // TODO: 2. Save that model to the hdfs as csv file
        // TODO: 3. Download locally that file
        // TODO: 4. Use that file for Python visualisation
        // TODO: 5. Create and train spark model with that Dataset
        groupedData.write().csv(hdfsUrl + "/big-data/data-new.csv");
        groupedData.show();

//        JavaRDD<Tuple2<String, String>> test = traffic.toJavaRDD().map(f -> {
//            weather.filter(weather.col("LocationLat").equalTo(f.get(9)).and(weather.col("LocationLng").equalTo(f.get(10))));

//            String qwe = "-1";
//            boolean a = false;
//            if (broadcastVar != null) {
//                a = broadcastVar.value().isEmpty();
//            } else {
//                qwe = "NULL";
//            }
//
//            if (a) {
//                qwe = "EMPTY";
//            }

//            if (a.length() == 0) {
//                return new Tuple2<>(f.getString(0), null);
//            }
//            return new Tuple2<>(f.getString(0), broadcastVar.value().get(0).getString(0));
//        });

//        JavaRDD<Row> javaRddRow = test.map(tup -> {
//            String a = tup._1();
//            String b = tup._2();
//            return RowFactory.create(a, b);
//        });
//
//        List<StructField> fields = new ArrayList<StructField>();
//        fields.add(DataTypes.createStructField("TrafficEventId", DataTypes.StringType, true));
//        fields.add(DataTypes.createStructField("WeatherEventId", DataTypes.StringType, true));
//        StructType schema = DataTypes.createStructType(fields);
//
//        Dataset<Row> converted = spark.createDataFrame(javaRddRow, schema);
//        converted.show(10000);

        spark.stop();
        spark.close();
    }

    public static double distance(String sLat1, String sLat2, String sLon1, String sLon2) {
        double lat1 = Double.parseDouble(sLat1);
        double lat2 = Double.parseDouble(sLat2);
        double lon1 = Double.parseDouble(sLon1);
        double lon2 = Double.parseDouble(sLon2);

        final int R = 6371; // Radius of the earth

        double latDistance = Math.toRadians(lat2 - lat1);
        double lonDistance = Math.toRadians(lon2 - lon1);
        double a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(lonDistance / 2) * Math.sin(lonDistance / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        double distanceInMeters = R * c * 1000;

        distanceInMeters = Math.pow(distanceInMeters, 2);

        return Math.sqrt(distanceInMeters);
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
