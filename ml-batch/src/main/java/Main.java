import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.feature.VectorIndexer;
import org.apache.spark.ml.feature.VectorIndexerModel;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF4;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.types.DataTypes.*;

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

        UDF1<String, Integer> udfConvertTrafficType = Main::convertTrafficEventToNumber;
        UserDefinedFunction getTrafficType = functions.udf(udfConvertTrafficType, DataTypes.IntegerType);

        UDF1<String, Integer> udfConvertWeatherType = Main::convertWeatherEventToNumber;
        UserDefinedFunction getWeatherType = functions.udf(udfConvertWeatherType, DataTypes.IntegerType);

        UDF1<String, Integer> udfConvertSeverity = Main::convertSeverityToNumber;
        UserDefinedFunction getSeverity = functions.udf(udfConvertSeverity, DataTypes.IntegerType);

//        Dataset<Row> groupedData = traffic.join(weather,
//                traffic.col("City").equalTo(weather.col("W-City"))
//                        .and(getDistance.apply(traffic.col("LocationLat"), weather.col("W-LocationLat"), traffic.col("LocationLng"), weather.col("W-LocationLng")).leq(5000.0))
//                        .and(traffic.col("StartTime(UTC)").leq(weather.col("W-StartTime(UTC)")).and(traffic.col("EndTime(UTC)").geq(weather.col("W-EndTime(UTC)")))
//                                .or(weather.col("W-StartTime(UTC)").leq(traffic.col("StartTime(UTC)")).and(weather.col("W-EndTime(UTC)").geq(traffic.col("EndTime(UTC)")))))
//        )
//                .select(traffic.col("EventId"), weather.col("W-EventId"),
//                        getTrafficType.apply(traffic.col("Type")).as("TrafficType"), getWeatherType.apply(weather.col("W-Type")).as("WeatherType"),
//                        getSeverity.apply(traffic.col("Severity")).as("TrafficSeverity"),
//                        getSeverity.apply(weather.col("W-Severity")).as("WeatherSeverity")
//                );

        Dataset<Row> groupedData = traffic.join(weather,
                traffic.col("City").equalTo(weather.col("W-City"))
                        .and(getDistance.apply(traffic.col("LocationLat"), weather.col("W-LocationLat"), traffic.col("LocationLng"), weather.col("W-LocationLng")).leq(5000.0))
                        .and(traffic.col("StartTime(UTC)").leq(weather.col("W-StartTime(UTC)")).and(traffic.col("EndTime(UTC)").geq(weather.col("W-EndTime(UTC)")))
                                .or(weather.col("W-StartTime(UTC)").leq(traffic.col("StartTime(UTC)")).and(weather.col("W-EndTime(UTC)").geq(traffic.col("EndTime(UTC)")))))
        )
                .select(
                        getTrafficType.apply(traffic.col("Type")).as("TrafficType"),
                        getWeatherType.apply(weather.col("W-Type")).as("WeatherType"),
                        getSeverity.apply(weather.col("W-Severity")).as("WeatherSeverity"),
                        weather.col("W-LocationLat").cast(DoubleType).as("Lat"),
                        weather.col("W-LocationLng").cast(DoubleType).as("Lng")
                );

        VectorAssembler vectorAssembler = new VectorAssembler()
                .setInputCols(new String[]{"WeatherType", "WeatherSeverity", "Lat", "Lng"})
                .setOutputCol("Features");

        Dataset<Row> transformed = vectorAssembler.transform(groupedData);

        // TODO: train model
        Dataset<Row>[] splits = transformed.randomSplit(new double[]{0.7, 0.3});
        Dataset<Row> trainingData = splits[0];
        Dataset<Row> testData = splits[1];
        VectorIndexerModel featureIndexer = new VectorIndexer()
                .setInputCol("Features")
                .setOutputCol("indexedFeatures")
                .setMaxCategories(4)
                .fit(transformed);

        RandomForestClassifier rf = new RandomForestClassifier()
                .setLabelCol("TrafficType")
                .setFeaturesCol("indexedFeatures");


        Pipeline pipeline = new Pipeline()
                .setStages(new PipelineStage[]{vectorAssembler, featureIndexer, rf});

        PipelineModel model = pipeline.fit(trainingData);

        Dataset<Row> predictions = model.transform(testData);
        predictions.show(100);

        spark.stop();
        spark.close();
    }

    public static int convertSeverityToNumber(String severity) {
        if (severity == null) {
            return 0;
        }
        switch (severity) {
            case "Light":
            case "Short":
            case "Fast":
                return 1;
            case "Moderate":
                return 2;
            case "Heavy":
                return 3;
            case "Severe":
            case "Slow":
            case "Long":
                return 4;
            default:
                return 0;
        }
    }

    public static int convertTrafficEventToNumber(String trafficType) {
        switch (trafficType) {
            case "Accident":
                return 0;
            case "Broken-Vehicle":
                return 1;
            case "Congestion":
                return 2;
            case "Construction":
                return 3;
            case "Event":
                return 4;
            case "Lane-Blocked":
                return 5;
            case "Flow-Incident":
                return 6;
            default:
                return -1;
        }
    }

    public static int convertWeatherEventToNumber(String weatherType) {
        switch (weatherType) {
            case "Severe-Cold":
                return 0;
            case "Fog":
                return 1;
            case "Hail":
                return 2;
            case "Rain":
                return 3;
            case "Snow":
                return 4;
            case "Storm":
                return 5;
            default:
                return 6;
        }
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
}
