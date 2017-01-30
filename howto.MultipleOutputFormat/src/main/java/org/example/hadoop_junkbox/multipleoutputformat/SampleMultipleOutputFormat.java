package org.example.hadoop_junkbox.multipleoutputformat;

import org.example.hadoop_junkbox.multipleoutputformat.rw.CsvRecordWriter;
import org.example.hadoop_junkbox.multipleoutputformat.rw.LineRecordWriter;
import org.example.hadoop_junkbox.multipleoutputformat.rw.PipeRecordWriter;
import org.example.hadoop_junkbox.multipleoutputformat.rw.TsvRecordWriter;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.lib.MultipleOutputFormat;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SampleMultipleOutputFormat extends MultipleOutputFormat<String, byte[]> {
    private static Logger logger = LoggerFactory.getLogger(SampleMultipleOutputFormat.class);

    private static final String KEY0 = "KEY000";
    private static final String KEY1 = "KEY001";
    private static final String KEY2 = "KEY002";
    private static final String KEY3 = "KEY003";
    private static final String KEY4 = "KEY004";
    private static final String KEY5 = "KEY005";
    private static final String KEY6 = "KEY006";
    private static final String KEY7 = "KEY007";
    private static final String KEY8 = "KEY008";
    private static final String KEY9 = "KEY009";

    private Map<String, RecordWriter<String, byte[]>> recordWriterMap = new HashMap<>();

    @Override
    protected String generateFileNameForKeyValue(String key, byte[] value, String name) {
        return new Path(key, name).toString();
    }

    @Override
    protected String generateActualKey(String key, byte[] value) {
        return null; // discard the key since it is already in the file path
    }

    @Override
    protected RecordWriter<String, byte[]> getBaseRecordWriter(
            FileSystem fs, JobConf job, String name, Progressable arg3) throws IOException {
        if (name == null) {
            logger.error("name was null");
            throw new IOException("Target name was null");
        }
        String key = name.substring(0, name.indexOf('/'));
        RecordWriter<String, byte[]> recordWriter = recordWriterMap.get(key);
        if (recordWriter != null) {
            return recordWriter;
        }
        Path file = FileOutputFormat.getTaskOutputPath(job, name);
        FSDataOutputStream out = fs.create(file, arg3);
        switch (key) {
            case KEY0:
                recordWriter = new LineRecordWriter(out);
                break;
            case KEY1:
                recordWriter = new LineRecordWriter(out);
                break;
            case KEY2:
                recordWriter = new PipeRecordWriter(out);
                break;
            case KEY3:
                recordWriter = new PipeRecordWriter(out);
                break;
            case KEY4:
                recordWriter = new CsvRecordWriter(out);
                break;
            case KEY5:
                recordWriter = new CsvRecordWriter(out);
                break;
            case KEY6:
                recordWriter = new TsvRecordWriter(out);
                break;
            case KEY7:
                recordWriter = new TsvRecordWriter(out);
                break;
            case KEY8:
                recordWriter = new LineRecordWriter(out);
                break;
            case KEY9:
                recordWriter = new LineRecordWriter(out);
                break;
            default:
                logger.error("unknown key `{}'", key);
                throw new IOException("Unsupported key specified");
        }
        recordWriterMap.put(key, recordWriter);
        return recordWriter;
    }
}
