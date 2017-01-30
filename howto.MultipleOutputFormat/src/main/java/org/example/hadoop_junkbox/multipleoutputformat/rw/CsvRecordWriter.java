package org.example.hadoop_junkbox.multipleoutputformat.rw;

import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class CsvRecordWriter implements RecordWriter<String, byte[]> {
    private static final String utf8 = "UTF-8";
    private static final byte[] comma;
    static {
        try {
            comma = ",".getBytes(utf8);
        }
        catch (UnsupportedEncodingException uee) {
            throw new IllegalArgumentException("can't find " + utf8 + " encoding");
        }
    }

    private DataOutputStream out;

    public CsvRecordWriter(DataOutputStream out) {
        this.out = out;
    }

    public synchronized void write(String key, byte[] value) throws IOException {
        boolean nullValue = value == null;
        if (!nullValue) {
            out.write(value, 0, value.length);
        }
        out.write(comma);
    }

    public synchronized void close(Reporter reporter) throws IOException {
        out.close();
    }
}
