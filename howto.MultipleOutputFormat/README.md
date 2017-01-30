# How to use MultipleOutputFormat&lt;K, V&gt; class by example
We can use MultipleOutputFormat&lt;K, V&gt; class from Hadoop Old API to generate multiple output files at once.

This toy project demonstrates the following: 
- Records in a pair RDD could be written into separate output files
- We can freely choose name of output files
- We can freely choose format of each output files, there's no restrictions on output format
  - MultipleTextOutputFormat&lt;K, V&gt; only allows us to write the output data in Text output format
  - MultipleSequenceFileOutputFormat&lt;K, V&gt; only allows us to write the output data in Sequence file output format

## How to run
Just clone this repository and run mvn from this toy project's top directory:
```
% git clone https://github.com/kmizumar/hadoop-junkbox.git
% cd hadoop-junkbox/howto.MultipleOutputFormat
% mvn test
```
It's quite simple, isn't it?

## Test scenario
Suppose we have a RDD of two items, String as a key and byte[] as a value.
In Java term, we have a RDD of type JavaRDD&lt;Tuple2&lt;String, byte[]&gt;&gt; and would like to save the values into several separate files.
It is desirable that an output file has a name which is easy to determine what kind of values are written inside.
In addition, we would like to have files in different file format.
The actual values in byte[] object have different meanings depending on its key.

## How it works
