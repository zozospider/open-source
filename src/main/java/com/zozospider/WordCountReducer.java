package com.zozospider;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Reducer
 */
// KEYIN: Reduce 阶段输入的 key (Map 阶段输出的 key)
// KEYOUT: Reduce 阶段输入的 value (Map 阶段输出的 value)
// KEYOUT: Reduce 阶段输出的 key
// VALUEOUT: Reduce 阶段输出的 value
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    IntWritable valueOut = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // key_in: abc
        // values_in: [1, 1]

        int sum = 0;

        // 1 累加求和
        for (IntWritable value : values) {
            sum += value.get();
        }
        valueOut.set(sum);

        // key_out: abc
        // value_out: 2

        // 2 Reduce 写出
        context.write(key, valueOut);
    }

}
