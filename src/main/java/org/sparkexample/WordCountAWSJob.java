package org.sparkexample;

import java.util.Arrays;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;

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

    private static final String PLATFORM_AWS = "AWS";
    private static final String LOCAL = "local";
    private static final String YARN_CLUSTER = "yarn-cluster";

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

        ArgumentParser parser =
            ArgumentParsers.newArgumentParser("Word Count Job").defaultHelp(true)
                .description("Runs Infutor Cleanser Job");
        parser.addArgument("-p", "--platform").type(String.class).setDefault(PLATFORM_AWS);
        parser.addArgument("-in", "--input").type(String.class).help("list of input directories");
        parser.addArgument("-out", "--output").type(String.class).help("list of output directories");
        parser.addArgument("-log", "--log").type(String.class).help("list of output directories");

        Namespace arguments = parser.parseArgsOrFail(args);
        System.out.println(arguments);

        // --------------------------------------------------------------------
        // Set up Spark Configuration
        SparkConf sparkConf = new SparkConf().setAppName("WordCount on AWS Spark");

        if (PLATFORM_AWS.equalsIgnoreCase(arguments.get("platform").toString())) {
            sparkConf.setMaster(YARN_CLUSTER);
        } else {
            sparkConf.setMaster(LOCAL);
        }

        sparkConf.set("spark.serializer", KryoSerializer.class.getName());
        // ---------------------------------------------------------------------
        // Set up Spark Context
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        final Accumulator<Integer> accum = ctx.accumulator(0);

        // ---------------------------------------------------------------------
        // Set up Map Reduce flow
        JavaRDD<String> lines = ctx.textFile(arguments.get("input").toString());
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            private static final long serialVersionUID = 1L;

            public Iterable<String> call(String s) {
                accum.add(1);
                return Arrays.asList(s.split(" "));
            }
        });
        JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long serialVersionUID = 1L;

            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = 1L;

            public Integer call(Integer a, Integer b) {
                return a + b;
            }
        });
        System.out.println("partitions : " + lines.partitions().size());

        // ----------------------------------------------------------------------
        // Execute Job
        long startTime = System.currentTimeMillis();

        if (SystemParameters.getJobPlatform() == SystemParameters.Platform.AWS) {
            // counts.coalesce(100, false).saveAsTextFile(sysPropertiesMap.get(OUTPUT_DATA), GzipCodec.class);
            counts.saveAsTextFile(arguments.get("output").toString(), GzipCodec.class);
        } else {
            counts.coalesce(100, false).saveAsTextFile(arguments.get("output").toString());
        }

        long runTime = (System.currentTimeMillis() - startTime) / 1000;
        System.out.println("Job completed, it took " + runTime + " seconds !!!");
        System.out.println("Total line numbers = " + accum);

        // -----------------------------------------------------------------------
        ctx.stop();
        ctx.close();
    }
}
