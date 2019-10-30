package com.zozospider;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * MapReduce 驱动: 统计单次出现次数
 */
public class WordCountDriver {

    /**
     * spiderxmac:input zoz$ ls -l /Users/zoz/zz/other/tmp/MapReduce/input/WordCount/
     * total 8
     * -rw-r--r--  1 zoz  staff  27 10 30 23:05 f1
     * spiderxmac:input zoz$ cat WordCount/f1
     * abc abc see
     * qq
     * love qq abc
     * spiderxmac:input zoz$
     * <p>
     * spiderxmac:output zoz$ ls -l /Users/zoz/zz/other/tmp/MapReduce/output/WordCount/
     * ls: /Users/zoz/zz/other/tmp/MapReduce/output/WordCount/: No such file or directory
     * spiderxmac:output zoz$
     */


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 本地运行时不要注释下面 1 行
        args = new String[]{"/Users/zoz/zz/other/tmp/MapReduce/input/WordCount", "/Users/zoz/zz/other/tmp/MapReduce/output/WordCount"};

        // 1 获取 Job 对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2 设置 Jar, Mapper, Reducer 类
        job.setJarByClass(WordCountDriver.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 3 设置 Map 阶段和最终的 KEYOUT, VALUEOUT
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 4 设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 5 提交 Job
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }


    /**
     * spiderxmac:output zoz$ ls -l /Users/zoz/zz/other/tmp/MapReduce/output/WordCount/
     * total 8
     * -rw-r--r--  1 zoz  staff   0 10 30 23:06 _SUCCESS
     * -rw-r--r--  1 zoz  staff  24 10 30 23:06 part-r-00000
     * spiderxmac:output zoz$ cat WordCount/part-r-00000
     * abc	3
     * love	1
     * qq	2
     * see	1
     * spiderxmac:output zoz$
     */

    /**
     * 2019-10-30 23:06:01,947 WARN [org.apache.hadoop.util.NativeCodeLoader] - Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
     * 2019-10-30 23:06:02,059 INFO [org.apache.hadoop.conf.Configuration.deprecation] - session.id is deprecated. Instead, use dfs.metrics.session-id
     * 2019-10-30 23:06:02,060 INFO [org.apache.hadoop.metrics.jvm.JvmMetrics] - Initializing JVM Metrics with processName=JobTracker, sessionId=
     * 2019-10-30 23:06:02,499 WARN [org.apache.hadoop.mapreduce.JobResourceUploader] - Hadoop command-line option parsing not performed. Implement the Tool interface and execute your application with ToolRunner to remedy this.
     * 2019-10-30 23:06:02,503 WARN [org.apache.hadoop.mapreduce.JobResourceUploader] - No job jar file set.  User classes may not be found. See Job or Job#setJar(String).
     * 2019-10-30 23:06:02,521 INFO [org.apache.hadoop.mapreduce.lib.input.FileInputFormat] - Total input paths to process : 1
     * 2019-10-30 23:06:02,573 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - number of splits:1
     * 2019-10-30 23:06:02,655 INFO [org.apache.hadoop.mapreduce.JobSubmitter] - Submitting tokens for job: job_local371366310_0001
     * 2019-10-30 23:06:02,811 INFO [org.apache.hadoop.mapreduce.Job] - The url to track the job: http://localhost:8080/
     * 2019-10-30 23:06:02,811 INFO [org.apache.hadoop.mapreduce.Job] - Running job: job_local371366310_0001
     * 2019-10-30 23:06:02,812 INFO [org.apache.hadoop.mapred.LocalJobRunner] - OutputCommitter set in config null
     * 2019-10-30 23:06:02,815 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-30 23:06:02,816 INFO [org.apache.hadoop.mapred.LocalJobRunner] - OutputCommitter is org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
     * 2019-10-30 23:06:02,840 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Waiting for map tasks
     * 2019-10-30 23:06:02,841 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local371366310_0001_m_000000_0
     * 2019-10-30 23:06:02,857 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-30 23:06:02,861 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-30 23:06:02,861 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-30 23:06:02,864 INFO [org.apache.hadoop.mapred.MapTask] - Processing split: file:/Users/zoz/zz/other/tmp/MapReduce/input/WordCount/f1:0+27
     * 2019-10-30 23:06:02,921 INFO [org.apache.hadoop.mapred.MapTask] - (EQUATOR) 0 kvi 26214396(104857584)
     * 2019-10-30 23:06:02,921 INFO [org.apache.hadoop.mapred.MapTask] - mapreduce.task.io.sort.mb: 100
     * 2019-10-30 23:06:02,921 INFO [org.apache.hadoop.mapred.MapTask] - soft limit at 83886080
     * 2019-10-30 23:06:02,921 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufvoid = 104857600
     * 2019-10-30 23:06:02,921 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396; length = 6553600
     * 2019-10-30 23:06:02,923 INFO [org.apache.hadoop.mapred.MapTask] - Map output collector class = org.apache.hadoop.mapred.MapTask$MapOutputBuffer
     * 2019-10-30 23:06:02,928 INFO [org.apache.hadoop.mapred.LocalJobRunner] -
     * 2019-10-30 23:06:02,928 INFO [org.apache.hadoop.mapred.MapTask] - Starting flush of map output
     * 2019-10-30 23:06:02,928 INFO [org.apache.hadoop.mapred.MapTask] - Spilling map output
     * 2019-10-30 23:06:02,928 INFO [org.apache.hadoop.mapred.MapTask] - bufstart = 0; bufend = 55; bufvoid = 104857600
     * 2019-10-30 23:06:02,928 INFO [org.apache.hadoop.mapred.MapTask] - kvstart = 26214396(104857584); kvend = 26214372(104857488); length = 25/6553600
     * 2019-10-30 23:06:02,933 INFO [org.apache.hadoop.mapred.MapTask] - Finished spill 0
     * 2019-10-30 23:06:02,936 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local371366310_0001_m_000000_0 is done. And is in the process of committing
     * 2019-10-30 23:06:02,941 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map
     * 2019-10-30 23:06:02,941 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local371366310_0001_m_000000_0' done.
     * 2019-10-30 23:06:02,942 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local371366310_0001_m_000000_0
     * 2019-10-30 23:06:02,942 INFO [org.apache.hadoop.mapred.LocalJobRunner] - map task executor complete.
     * 2019-10-30 23:06:02,943 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Waiting for reduce tasks
     * 2019-10-30 23:06:02,943 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Starting task: attempt_local371366310_0001_r_000000_0
     * 2019-10-30 23:06:02,948 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - File Output Committer Algorithm version is 1
     * 2019-10-30 23:06:02,948 INFO [org.apache.hadoop.yarn.util.ProcfsBasedProcessTree] - ProcfsBasedProcessTree currently is supported only on Linux.
     * 2019-10-30 23:06:02,948 INFO [org.apache.hadoop.mapred.Task] -  Using ResourceCalculatorProcessTree : null
     * 2019-10-30 23:06:02,949 INFO [org.apache.hadoop.mapred.ReduceTask] - Using ShuffleConsumerPlugin: org.apache.hadoop.mapreduce.task.reduce.Shuffle@7505f9ee
     * 2019-10-30 23:06:02,958 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - MergerManager: memoryLimit=2672505600, maxSingleShuffleLimit=668126400, mergeThreshold=1763853824, ioSortFactor=10, memToMemMergeOutputsThreshold=10
     * 2019-10-30 23:06:02,960 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - attempt_local371366310_0001_r_000000_0 Thread started: EventFetcher for fetching Map Completion Events
     * 2019-10-30 23:06:02,986 INFO [org.apache.hadoop.mapreduce.task.reduce.LocalFetcher] - localfetcher#1 about to shuffle output of map attempt_local371366310_0001_m_000000_0 decomp: 71 len: 75 to MEMORY
     * 2019-10-30 23:06:02,996 INFO [org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput] - Read 71 bytes from map-output for attempt_local371366310_0001_m_000000_0
     * 2019-10-30 23:06:02,996 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - closeInMemoryFile -> map-output of size: 71, inMemoryMapOutputs.size() -> 1, commitMemory -> 0, usedMemory ->71
     * 2019-10-30 23:06:02,997 INFO [org.apache.hadoop.mapreduce.task.reduce.EventFetcher] - EventFetcher is interrupted.. Returning
     * 2019-10-30 23:06:02,998 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 1 / 1 copied.
     * 2019-10-30 23:06:02,998 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - finalMerge called with 1 in-memory map-outputs and 0 on-disk map-outputs
     * 2019-10-30 23:06:03,002 INFO [org.apache.hadoop.mapred.Merger] - Merging 1 sorted segments
     * 2019-10-30 23:06:03,002 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 1 segments left of total size: 65 bytes
     * 2019-10-30 23:06:03,003 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merged 1 segments, 71 bytes to disk to satisfy reduce memory limit
     * 2019-10-30 23:06:03,003 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 1 files, 75 bytes from disk
     * 2019-10-30 23:06:03,003 INFO [org.apache.hadoop.mapreduce.task.reduce.MergeManagerImpl] - Merging 0 segments, 0 bytes from memory into reduce
     * 2019-10-30 23:06:03,003 INFO [org.apache.hadoop.mapred.Merger] - Merging 1 sorted segments
     * 2019-10-30 23:06:03,004 INFO [org.apache.hadoop.mapred.Merger] - Down to the last merge-pass, with 1 segments left of total size: 65 bytes
     * 2019-10-30 23:06:03,005 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 1 / 1 copied.
     * 2019-10-30 23:06:03,015 INFO [org.apache.hadoop.conf.Configuration.deprecation] - mapred.skip.on is deprecated. Instead, use mapreduce.job.skiprecords
     * 2019-10-30 23:06:03,019 INFO [org.apache.hadoop.mapred.Task] - Task:attempt_local371366310_0001_r_000000_0 is done. And is in the process of committing
     * 2019-10-30 23:06:03,020 INFO [org.apache.hadoop.mapred.LocalJobRunner] - 1 / 1 copied.
     * 2019-10-30 23:06:03,020 INFO [org.apache.hadoop.mapred.Task] - Task attempt_local371366310_0001_r_000000_0 is allowed to commit now
     * 2019-10-30 23:06:03,021 INFO [org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter] - Saved output of task 'attempt_local371366310_0001_r_000000_0' to file:/Users/zoz/zz/other/tmp/MapReduce/output/WordCount/_temporary/0/task_local371366310_0001_r_000000
     * 2019-10-30 23:06:03,021 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce > reduce
     * 2019-10-30 23:06:03,022 INFO [org.apache.hadoop.mapred.Task] - Task 'attempt_local371366310_0001_r_000000_0' done.
     * 2019-10-30 23:06:03,022 INFO [org.apache.hadoop.mapred.LocalJobRunner] - Finishing task: attempt_local371366310_0001_r_000000_0
     * 2019-10-30 23:06:03,023 INFO [org.apache.hadoop.mapred.LocalJobRunner] - reduce task executor complete.
     * 2019-10-30 23:06:03,816 INFO [org.apache.hadoop.mapreduce.Job] - Job job_local371366310_0001 running in uber mode : false
     * 2019-10-30 23:06:03,817 INFO [org.apache.hadoop.mapreduce.Job] -  map 100% reduce 100%
     * 2019-10-30 23:06:03,817 INFO [org.apache.hadoop.mapreduce.Job] - Job job_local371366310_0001 completed successfully
     * 2019-10-30 23:06:03,823 INFO [org.apache.hadoop.mapreduce.Job] - Counters: 30
     * 	File System Counters
     * 		FILE: Number of bytes read=588
     * 		FILE: Number of bytes written=549425
     * 		FILE: Number of read operations=0
     * 		FILE: Number of large read operations=0
     * 		FILE: Number of write operations=0
     * 	Map-Reduce Framework
     * 		Map input records=3
     * 		Map output records=7
     * 		Map output bytes=55
     * 		Map output materialized bytes=75
     * 		Input split bytes=122
     * 		Combine input records=0
     * 		Combine output records=0
     * 		Reduce input groups=4
     * 		Reduce shuffle bytes=75
     * 		Reduce input records=7
     * 		Reduce output records=4
     * 		Spilled Records=14
     * 		Shuffled Maps =1
     * 		Failed Shuffles=0
     * 		Merged Map outputs=1
     * 		GC time elapsed (ms)=7
     * 		Total committed heap usage (bytes)=514850816
     * 	Shuffle Errors
     * 		BAD_ID=0
     * 		CONNECTION=0
     * 		IO_ERROR=0
     * 		WRONG_LENGTH=0
     * 		WRONG_MAP=0
     * 		WRONG_REDUCE=0
     * 	File Input Format Counters
     * 		Bytes Read=27
     * 	File Output Format Counters
     * 		Bytes Written=36
     *
     * Process finished with exit code 0
     */

}
