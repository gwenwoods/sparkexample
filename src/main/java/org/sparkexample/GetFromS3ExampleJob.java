package org.sparkexample;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
import org.sparkexample.PlatformUtil.PlatformEnum;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import scala.Tuple2;

/**
 * 
 * @author wen
 *
 */
public final class GetFromS3ExampleJob {

    private static final String PLATFORM_AWS = "AWS";
    private static final String LOCAL = "local";
    private static final String YARN_CLUSTER = "yarn-cluster";

    private static List<String> myDataSetFroms3;

    private static List<String> myDataSetFromLocal;

    static {

        myDataSetFroms3 = new ArrayList<String>();
        // // String path = SystemParameters.getPlatformPath("peopleDirNames/PeopleDirectoryNames_A-J.csv");
        // String path = PlatformUtil.getPlatformPath(PlatformEnum.UNIX, "input/pg5000.txt");
        // InputStream fstream = PlatformUtil.getPlatformInputStream(PlatformEnum.UNIX, path);
        //
        // System.out.println("Hello  path = " + path + ", fstream = " + fstream);
        // DataInputStream in = new DataInputStream(fstream);
        // BufferedReader br = new BufferedReader(new InputStreamReader(in));
        //
        // String strLine;
        // int count = 0;
        // while ((strLine = br.readLine()) != null) {
        // count++;
        // String[] data = strLine.split(",");
        //
        // }
        // br.close();

        AmazonS3 s3Client = new AmazonS3Client(new ProfileCredentialsProvider());
        S3Object object =                     
            s3Client.getObject(new GetObjectRequest("s3://inflection-rl-east",
                "/wen/datasets/usps/TigerZip/data"));

        System.out.println("s3 object = " + object);
        InputStream objectData = object.getObjectContent();
        // Process the objectData stream.

        // System.out.println("Hello  path = " + path + ", fstream = " + fstream);
        DataInputStream in = new DataInputStream(objectData);
        BufferedReader br = new BufferedReader(new InputStreamReader(in));

        String strLine;
        int count = 0;
        try {
            while ((strLine = br.readLine()) != null) {
                count++;
                String[] data = strLine.split(",");
                myDataSetFroms3.add(data[0]);
                br.close();

                System.out.println("s3 InputStream objectData = " + objectData);
                objectData.close();


            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        // System.out.println(count);

    }

    /**
     * Private constructor.
     */
    private GetFromS3ExampleJob() {
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

        JavaRDD<String> lines = ctx.parallelize(myDataSetFroms3);
        // JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
        // private static final long serialVersionUID = 1L;
        //
        // public Iterable<String> call(String s) {
        // accum.add(1);
        // return Arrays.asList(s.split(" "));
        // }
        // });
        // JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
        // private static final long serialVersionUID = 1L;
        //
        // public Tuple2<String, Integer> call(String s) {
        // return new Tuple2<String, Integer>(s, 1);
        // }
        // });
        // JavaPairRDD<String, Integer> counts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
        // private static final long serialVersionUID = 1L;
        //
        // public Integer call(Integer a, Integer b) {
        // return a + b;
        // }
        // });
        System.out.println("partitions : " + lines.partitions().size());

        // ----------------------------------------------------------------------
        // Execute Job
        long startTime = System.currentTimeMillis();

        if (SystemParameters.getJobPlatform() == SystemParameters.Platform.AWS) {
            // counts.coalesce(100, false).saveAsTextFile(sysPropertiesMap.get(OUTPUT_DATA), GzipCodec.class);
            lines.saveAsTextFile(arguments.get("output").toString(), GzipCodec.class);
        } else {
            lines.coalesce(100, false).saveAsTextFile(arguments.get("output").toString());
        }

        long runTime = (System.currentTimeMillis() - startTime) / 1000;
        System.out.println("Job completed, it took " + runTime + " seconds !!!");
        System.out.println("Total line numbers = " + accum);

        // -----------------------------------------------------------------------
        ctx.stop();
        ctx.close();
    }
}
