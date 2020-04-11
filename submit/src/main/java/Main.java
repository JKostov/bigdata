import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import scala.Function1;
import scala.util.matching.Regex;

import java.util.HashMap;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Main {
    static HashMap<String, Integer> map = InitMap();

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

        Dataset<Row> dataSet = spark.read().option("header", "true").csv(csvFile);

        ShowTrafficEventTypesCountByCityandTimeSpan(dataSet);

        ShowDistanceOfRoadAffectedPerTypeData(dataSet);

        ShowCityWithTheMaxandMinNumberOfAccidentsInPeriod(dataSet);

        ShowAverageCongestionDurationPerCityInPeriod(dataSet);

        spark.stop();
        spark.close();
    }

    static void ShowTrafficEventTypesCountByCityandTimeSpan(Dataset<Row> ds) {
        String city = "Salt Lake City";
        String startTime = "2017-00-00 00:00:00";
        String endTime = "2018-00-00 00:00:00";
        int greaterThan = 500;

        ds.filter(
                ds.col("Source").equalTo("T")
                        .and(ds.col("City").equalTo(city))
                        .and(
                                ds.col("StartTime(UTC)").gt(functions.lit(startTime))
                                        .and(ds.col("EndTime(UTC)").lt(functions.lit(endTime)))
                        )
        )
                .groupBy("Type")
                .agg(functions.count(ds.col("Type")))
                .filter(functions.count(ds.col("Type")).gt(greaterThan))
                .show()
        ;
    }

    static void ShowDistanceOfRoadAffectedPerTypeData(Dataset<Row> ds) {
        String city = "San Francisco";
        String startDate = "2010-01-01 00:00:00";
        String endDate = "2020-01-01 00:00:00";

        ds.filter(
                ds.col("Source").equalTo("T")
                        .and(ds.col("City").equalTo(city))
                        .and(ds.col("StartTime(UTC)").between(startDate, endDate))
                        .and(ds.col("EndTime(UTC)").between(startDate, endDate))
        )
                .groupBy("Type")
                .agg(functions.min("Distance(mi)"), functions.max("Distance(mi)"), functions.avg("Distance(mi)"), functions.stddev("Distance(mi)"))
                .show()
        ;
    }

    static void ShowCityWithTheMaxandMinNumberOfAccidentsInPeriod(Dataset<Row> ds) {
        String startDate = "2010-01-01 00:00:00";
        String endDate = "2020-01-01 00:00:00";
        String eventType = "Accident";

        ds.filter(
                ds.col("Source").equalTo("T")
                        .and(ds.col("Type").equalTo(eventType))
                        .and(ds.col("StartTime(UTC)").between(startDate, endDate))
                        .and(ds.col("EndTime(UTC)").between(startDate, endDate))
        )
                .groupBy("City")
                .agg(functions.count("City"))
                .agg(functions.min("City"), functions.max("City"))
                .show()
        ;
    }

    static void ShowAverageCongestionDurationPerCityInPeriod(Dataset<Row> ds) {
        String startDate = "2010-01-01 00:00:00";
        String endDate = "2020-01-01 00:00:00";
        String eventType = "Congestion";

        UDF1<String, Integer> udfConvert = (descriptionString) -> {
            Pattern r = Pattern.compile("(\\w*) (minutes|minute)");
            Matcher m = r.matcher(descriptionString);

            if (m.find()) {
                String numberString = m.group(1);
                return ConvertToNumber(numberString);
            } else {
                return 0;
            }
        };
        UserDefinedFunction extractMinutesFromDescription = functions.udf(udfConvert, DataTypes.IntegerType);

        ds.filter(
                ds.col("Source").equalTo("T")
                        .and(ds.col("Type").equalTo(eventType))
                        .and(ds.col("StartTime(UTC)").between(startDate, endDate))
                        .and(ds.col("EndTime(UTC)").between(startDate, endDate))
        )
                .groupBy("City")
                .agg(functions.avg(extractMinutesFromDescription.apply(ds.col("Description"))))
                .show()
        ;
    }

    static int ConvertToNumber(String numberString) {
        Integer num = tryParse(numberString);
        if (num != null)
        {
            return num;
        }

        String[] split = numberString.replace('-', ' ').split(" ");
        if (split.length > 2) {
            return 0;
        }

        if (split.length == 2) {
            int num1 = map.getOrDefault(split[0], 0);
            int num2 = map.getOrDefault(split[1], 0);

            return num1 + num2;
        }

        int number = map.getOrDefault(split[0], 0);

        return number;
    }

    static Integer tryParse(String numberString) {
        Integer retVal;
        try {
            retVal = Integer.parseInt(numberString);
        } catch (NumberFormatException nfe) {
            retVal = null;
        }
        return retVal;
    }

    static HashMap<String, Integer> InitMap() {
        HashMap<String, Integer> map = new HashMap<>();
        map.put("one", 1);
        map.put("two", 2);
        map.put("three", 3);
        map.put("four", 4);
        map.put("five", 5);
        map.put("six", 6);
        map.put("seven", 7);
        map.put("eight", 8);
        map.put("nine", 9);
        map.put("ten", 10);
        map.put("eleven", 11);
        map.put("twelve", 12);
        map.put("thirteen", 13);
        map.put("fourteen", 14);
        map.put("fifteen", 15);
        map.put("sixteen", 16);
        map.put("seventeen", 17);
        map.put("eighteen", 18);
        map.put("nineteen", 19);
        map.put("twenty", 20);
        map.put("thirty", 30);
        map.put("forty", 40);
        map.put("fifty", 50);
        map.put("sixty", 60);
        map.put("seventy", 70);
        map.put("eighty", 80);
        map.put("ninety", 90);

        return map;
    }
}
