import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;

import java.time.Duration;
import java.time.Instant;
import java.util.*;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.RandomForest;
import org.apache.spark.mllib.tree.model.RandomForestModel;
import org.apache.spark.mllib.util.MLUtils;
import scala.Tuple2;

public class ReadFile {
    static class SplitFunction implements FlatMapFunction<String, String> {
        private static final long serialVersionUID = 1L;

        @Override
        public Iterator<String> call(String s) {
            List<String> t2Arr = new ArrayList<>();
            if(!s.contains("##") || !s.contains("#")) {
                s = s.trim().replaceAll("\\s+", " ");
                List<String> tArr = Arrays.asList(s.split(" "));
                String s2 = tArr.get(tArr.size() - 1);
                t2Arr = Arrays.asList(s2.split(";"));
            }
            return t2Arr.iterator();
        }
    }

    public static void main(String[] args) {
        Instant startTime = Instant.now();
        SparkConf sparkConf = new SparkConf().setAppName("Read Text to RDD")
            .setMaster("local[*]").set("spark.executor.memory", "12g").set("spark.driver.maxResultSize", "12g");
        // start a spark context
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        // provide path to input text file
        String path = "/Users/nguyenthoai/Desktop/big/test_gnom.vcf";
        JavaRDD<String> textFile = sparkContext.textFile(path);



        JavaRDD<String> words = textFile.flatMap(new SplitFunction());





        /*Below code generates Pair of Word with count as one
         *similar to Mapper in Hadoop MapReduce*/
        JavaPairRDD<String, Integer> pairs = words
            .mapToPair(new PairFunction<String, String, Integer>() {
                public Tuple2<String, Integer> call(String s) {
                    return new Tuple2<String, Integer>(s, 1);
                }
            });







        /*Below code aggregates Pairs of Same Words with count
         *similar to Reducer in Hadoop MapReduce
         */

        JavaPairRDD<String, Integer> counts = pairs.reduceByKey(
            new Function2<Integer, Integer, Integer>() {
                public Integer call(Integer a, Integer b) {
                    return a + b;
                }
            });

        




        counts.saveAsTextFile("/Users/nguyenthoai/Desktop/big/test_gnom");


        sparkContext.stop();
        sparkContext.close();

        Instant endTime = Instant.now();
        Duration du = Duration.between(startTime, endTime);
        System.out.println("Time executive ===" + du.getSeconds());


    }
}
