package org.sparkexample;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.serializer.KryoSerializer;

import scala.Tuple2;

/**
 * 
 * @author wen
 *
 */
public final class WordCountAWSJob {

    private static final String PLATFORM = "PLATFORM";
    private static final String CLUSTER_MODE = "CLUSTER.MODE";
    private static final String DEPENDENCY_JARS = "DEPENDENCY.JARS";
    private static final String LOCAL = "local";
    private static final String YARN_CLUSTER = "yarn-cluster";
    private static final String INPUT_DATA = "INPUT.DATA";
    private static final String OUTPUT_DATA = "OUTPUT.DATA";

    /**
     * Private constructor.
     */
    private WordCountAWSJob() {
    }

    /**
     * 
     * @param args
     *            input_profile_folder output_result_folder
     * 
     */
    public static void main(String[] args) {

        // -------------------------------------------------------------------
        // Parse Job Configuration

        Map<String, String> sysPropertiesMap = new HashMap<String, String>();

        if (SystemParameters.getJobPlatform() == SystemParameters.Platform.AWS) {
            sysPropertiesMap.put(PLATFORM, "AWS");
            sysPropertiesMap.put(CLUSTER_MODE, YARN_CLUSTER);
        } else {
            sysPropertiesMap.put(PLATFORM, "UNIX");
            sysPropertiesMap.put(CLUSTER_MODE, LOCAL);
            // sysPropertiesMap.put(DEPENDENCY_JARS, "s3n://path/dependency.jar");
        }

        sysPropertiesMap.put(INPUT_DATA, args[0]);
        sysPropertiesMap.put(OUTPUT_DATA, args[1]);

        // --------------------------------------------------------------------
        // Set up Spark Configuration
        SparkConf sparkConf = new SparkConf().setAppName("WordCount on AWS Spark");

        sparkConf.setMaster(sysPropertiesMap.get(CLUSTER_MODE));
        if (sysPropertiesMap.get(CLUSTER_MODE).equals(LOCAL)) {
            sparkConf.set("spark.executor.memory", "2g");
        } else {
            if (sysPropertiesMap.get(DEPENDENCY_JARS) != null) {
                sparkConf.setJars(new String[]{ sysPropertiesMap.get(DEPENDENCY_JARS), });
            }
        }

        sparkConf.set("spark.serializer", KryoSerializer.class.getName());
        // ---------------------------------------------------------------------
        // Set up Spark Context
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        final Accumulator<Integer> accum = ctx.accumulator(0);

        if (sysPropertiesMap.get(CLUSTER_MODE).equals(YARN_CLUSTER)) {
            if (sysPropertiesMap.get(DEPENDENCY_JARS) != null) {
                ctx.addJar(sysPropertiesMap.get(DEPENDENCY_JARS));
            }
        }

        // ---------------------------------------------------------------------
        // Set up Map Reduce flow
        JavaRDD<String> lines = ctx.textFile(sysPropertiesMap.get(INPUT_DATA));
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            public Iterable<String> call(String s) {
                accum.add(1);
                return Arrays.asList(s.split(" "));
            }
        });
        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer a, Integer b) {
                return a + b;
            }
        });
        System.out.println("partitions : " + lines.partitions().size());

        // ----------------------------------------------------------------------
        // Execute Job
        long startTime = System.currentTimeMillis();

        if (SystemParameters.getJobPlatform() == SystemParameters.Platform.AWS) {
            counts.coalesce(100, false).saveAsTextFile(sysPropertiesMap.get(OUTPUT_DATA), GzipCodec.class);
        } else {
            counts.coalesce(1, false).saveAsTextFile(sysPropertiesMap.get(OUTPUT_DATA));
        }

        long runTime = (System.currentTimeMillis() - startTime) / 1000;
        System.out.println("Job completed, it took " + runTime + " seconds !!!");
        System.out.println("Total line numbers = " + accum);

        // -----------------------------------------------------------------------
        ctx.stop();
        ctx.close();
    }
}
