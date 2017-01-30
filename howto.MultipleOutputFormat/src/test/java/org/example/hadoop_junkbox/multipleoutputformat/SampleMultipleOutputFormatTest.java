package org.example.hadoop_junkbox.multipleoutputformat;

import junitx.framework.FileAssert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

public class SampleMultipleOutputFormatTest {
    private static final Logger logger = LoggerFactory.getLogger(SampleMultipleOutputFormatTest.class);
    private static File tempDir;
    private static MiniDFSCluster hdfsCluster;
    private static String hdfsURI;
    private static FileSystem hdfsFileSystem;
    private static JavaSparkContext sc;
    public SampleMultipleOutputFormatTest() {
        // DO NOTHING FOR NOW
    }
    @Test
    public void test() throws IOException {
        final int numKeys = 10;
        final int valSize = 256;
        final int codeA = "A".codePointAt(0);

        List<String> keys = Arrays.asList(IntStream.range(0, numKeys)
                .mapToObj(n -> String.format("KEY%03d", n))
                .toArray(String[]::new));
        List<byte[]> vals = Arrays.asList(IntStream.range(0, numKeys)
                .mapToObj(n -> {
                    byte[] val = new byte[valSize];
                    Arrays.fill(val, (byte) (codeA + n));
                    return val;
                }).toArray(byte[][]::new));
        List<Tuple2<String, byte[]>> seed = new ArrayList<>(numKeys);
        {
            Iterator<String> it0 = keys.iterator();
            Iterator<byte[]> it1 = vals.iterator();
            while (it0.hasNext() && it1.hasNext()) {
                seed.add(new Tuple2<>(it0.next(), it1.next()));
            }
            assertFalse("size mismatch", it0.hasNext() || it1.hasNext());
        }
        JavaRDD<Tuple2<String, byte[]>> rdd0 = sc.parallelize(seed);
        JavaPairRDD<String, byte[]> rdd1 = rdd0
                .union(rdd0).union(rdd0).union(rdd0)
                .union(rdd0).union(rdd0).union(rdd0)
                .union(rdd0).union(rdd0).union(rdd0)
                .mapToPair(tup -> tup);
        final Partitioner partitioner = new HashPartitioner(numKeys);
        {
            Map<Integer, String> p2kMap = new HashMap<>(numKeys);
            keys.forEach(key -> {
                int partition = partitioner.getPartition(key);
                assertNull("found conflict", p2kMap.get(partition));
                p2kMap.put(partition, key);
            });
        }
        JavaPairRDD<String, byte[]> rdd2 = rdd1.partitionBy(partitioner);
        final String outPath = hdfsURI + "output/path";
        rdd2.saveAsHadoopFile(outPath, String.class, byte[].class,
                SampleMultipleOutputFormat.class);
        RemoteIterator<LocatedFileStatus> it =
                hdfsFileSystem.listFiles(new Path(outPath), true);
        HashMap<String, String> k2f = new HashMap<>();
        k2f.put("KEY000", "key000-line");
        k2f.put("KEY001", "key001-line");
        k2f.put("KEY002", "key002-pipe");
        k2f.put("KEY003", "key003-pipe");
        k2f.put("KEY004", "key004-csv");
        k2f.put("KEY005", "key005-csv");
        k2f.put("KEY006", "key006-tsv");
        k2f.put("KEY007", "key007-tsv");
        k2f.put("KEY008", "key008-line");
        k2f.put("KEY009", "key009-line");
        while (it.hasNext()) {
            LocatedFileStatus status = it.next();
            Path keyPath = status.getPath().getParent();
            String key = keyPath.getName();
            String vectorFile = k2f.get(key);
            if (vectorFile != null) {
                Path testFile = new Path(tempDir.toString(), status.getPath().getName());
                hdfsFileSystem.copyToLocalFile(status.getPath(), testFile);
                FileAssert.assertBinaryEquals(
                        new File(SampleMultipleOutputFormatTest.class
                                .getResource("/results/" + vectorFile).getFile()),
                        new File(testFile.toString()));
            }
            else {
                assertEquals("_SUCCESS", status.getPath().getName());
            }
        }
    }
    @BeforeClass
    public static void execBeforeClass() throws IOException {
        logger.debug("starting MiniDFSCluster for UnitTest");
        System.clearProperty(MiniDFSCluster.PROP_TEST_BUILD_DATA);
        File baseDir = new File(SampleMultipleOutputFormatTest.class
                .getResource("/testcluster").getFile());
        tempDir = new File(baseDir, "temp").getAbsoluteFile();
        File hdfsDir = new File(baseDir, "hdfs").getAbsoluteFile();
        FileUtil.fullyDelete(tempDir);
        FileUtil.fullyDelete(hdfsDir);
        Configuration conf = new HdfsConfiguration();
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, hdfsDir.getAbsolutePath());
        hdfsCluster = new MiniDFSCluster.Builder(conf).build();
        hdfsURI = "hdfs://localhost:" + hdfsCluster.getNameNodePort() + "/";
        hdfsFileSystem = FileSystem.get(conf);

        logger.debug("creating JavaSparkContext for UnitTest");
        SparkConf sparkConf = new SparkConf()
                .setAppName(SampleMultipleOutputFormatTest.class.getName())
                .setMaster("local[*]")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.io.compression.codec", "lz4");
        sc = new JavaSparkContext(sparkConf);
    }
    @AfterClass
    public static void execAfterClass() {
        logger.debug("shutting down the MiniDFSCluster");
        if (hdfsCluster != null) {
            hdfsCluster.shutdown();
            hdfsCluster = null;
        }
        logger.debug("closing the JavaSparkContext");
        if (sc != null) {
            sc.close();
            sc = null;
        }
    }
}
