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
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.RandomForest;
import org.apache.spark.mllib.tree.model.RandomForestModel;
import org.apache.spark.mllib.util.MLUtils;
import org.apache.spark.serializer.KryoSerializer;

import scala.Tuple2;

/**
 * 
 * @author wen
 *
 */
public final class RFModelJob {

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
    private RFModelJob() {
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

        String datapath = "data/mllib/sample_libsvm_data.txt";
        JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(ctx.sc(), datapath).toJavaRDD();
        // Split the data into training and test sets (30% held out for testing)
        JavaRDD<LabeledPoint>[] splits = data.randomSplit(new double[]{ 0.7, 0.3 });
        JavaRDD<LabeledPoint> trainingData = splits[0];
        JavaRDD<LabeledPoint> testData = splits[1];

        // Train a RandomForest model.
        // Empty categoricalFeaturesInfo indicates all features are continuous.
        Integer numClasses = 2;
        HashMap<Integer, Integer> categoricalFeaturesInfo = new HashMap<Integer, Integer>();
        Integer numTrees = 3; // Use more in practice.
        String featureSubsetStrategy = "auto"; // Let the algorithm choose.
        String impurity = "gini";
        Integer maxDepth = 5;
        Integer maxBins = 32;
        Integer seed = 12345;

        final RandomForestModel model =
            RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo, numTrees,
                featureSubsetStrategy, impurity, maxDepth, maxBins, seed);

        // Evaluate model on test instances and compute test error
        JavaPairRDD<Double, Double> predictionAndLabel =
            testData.mapToPair(new PairFunction<LabeledPoint, Double, Double>() {
                @Override
                public Tuple2<Double, Double> call(LabeledPoint p) {
                    return new Tuple2<Double, Double>(model.predict(p.features()), p.label());
                }
            });
        Double testErr = 1.0 * predictionAndLabel.filter(new Function<Tuple2<Double, Double>, Boolean>() {
            @Override
            public Boolean call(Tuple2<Double, Double> pl) {
                return !pl._1().equals(pl._2());
            }
        }).count() / testData.count();

        System.out.println("partitions : " + data.partitions().size());
        System.out.println("training count : " + trainingData.count());
        System.out.println("testing count : " + testData.count());

        // ----------------------------------------------------------------------
        // Execute Job
        long startTime = System.currentTimeMillis();

        if (SystemParameters.getJobPlatform() == SystemParameters.Platform.AWS) {
            // counts.coalesce(100, false).saveAsTextFile(sysPropertiesMap.get(OUTPUT_DATA), GzipCodec.class);
            predictionAndLabel.saveAsTextFile(sysPropertiesMap.get(OUTPUT_DATA), GzipCodec.class);
        } else {
            // counts.coalesce(100, false).saveAsTextFile(sysPropertiesMap.get(OUTPUT_DATA));
            predictionAndLabel.coalesce(100, false).saveAsTextFile(sysPropertiesMap.get(OUTPUT_DATA));
        }

        long runTime = (System.currentTimeMillis() - startTime) / 1000;
        System.out.println("Job completed, it took " + runTime + " seconds !!!");
        System.out.println("Total line numbers = " + accum);

        // -----------------------------------------------------------------------
        ctx.stop();
        ctx.close();
    }
}
