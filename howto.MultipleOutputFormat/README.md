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

### RDD
Simple pair RDD.
- Key is type of String
  - In this toy project, we use 10 keys starting from "KEY000" to "KEY009"
- Value is type of byte[]
  - The size of each value object is 256
  - Each value object is filled with a single value, 'A' + key index
  - For example, value for the key "KEY000" is filled with 256 'A's, "KEY001" is filled with 256 'B's, "KEY002" -> 'C's, and so on

### Output directories
Output files for a "KEY00x" will be located under the directory named "KEY00x". Record values which have "KEY000" will be written under the directory named "KEY000".

```
output/path/KEY000/
            +- part-00000
            +- part-00001
            |  ...
            +- part-xxxxx
```

### Output formats
For demonstration purpose, there're very simple, tiny implementation for line separated, pipe separated, comma separated, and tab separated data file formats.
- LineRecordWriter
  - Eacn values are separated by a newline '\n' character
  - Values for "KEY000", "KEY001", "KEY008" and "KEY009" will be written in this line separated format
- PipeRecordWriter
  - Each values are separated by a pipe '|' character
  - Values for "KEY002" and "KEY003" will be written in this pipe separated format
- CsvRecordWriter
  - Each values are separated by a comma ',' character
  - Values for "KEY004" and "KEY005" will be written in this comma separated format
- TsvRecordWriter
  - Each values are separated by a tab '\t' character
  - Values for "KEY006" and "KEY007" will be written in this tab separated format
 
![SampleMultipleOutputFormatTest workflow]
(https://raw.githubusercontent.com/kmizumar/hadoop-junkbox/9b9c514c434f016258caaa98424617d956cf01e9/images/figure000.png)

## How it works
SampleMultipleOutputFormat extends MultipleOutputFormat&lt;K, V&gt; and achieves the above requirements in the following way:

### Override generateFileNameForKeyValue to send a record to your desired destination
This method will be called by the framework with arguments such as "KEY000", 'AAAAA....' and "part-00001".
"part-00001" is the original filename suggested by the framework.
We can freely change the filename where this (key, value) record should be written to.

In this toy project, we can just add a directory entry to the suggested filename.
So if the key passed is "KEY000" and name passed is "part-00001" then we can simply return "KEY000/part-00001" as the desired output filename.
The value argument is not used here.

```
    @Override
    protected String generateFileNameForKeyValue(String key, byte[] value, String name) {
        return new Path(key, name).toString();
    }
```
