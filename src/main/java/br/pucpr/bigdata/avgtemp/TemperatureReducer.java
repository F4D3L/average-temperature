package br.pucpr.bigdata.avgtemp;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reduce phase: receives {@code ("temperature", [(t1, 1), (t2, 1), ...])} after sort/shuffle and
 * emits {@code ("temperature", average)}.
 *
 * <p>Input key/value: {@link Text} / {@link SumCountWritable}. Output key/value:
 * {@link Text} / {@link DoubleWritable}.
 *
 * <p>The formula is {@code (sum of all temperatures) / (number of readings)}: both totals are
 * accumulated first and divided once at the end. Averaging partial averages would give a wrong
 * result whenever the parts hold different numbers of readings (Lecture 05).
 */
public class TemperatureReducer extends Reducer<Text, SumCountWritable, Text, DoubleWritable> {

    @Override
    protected void reduce(Text key, Iterable<SumCountWritable> values, Context context)
            throws IOException, InterruptedException {
        double sum = 0;
        long count = 0;
        // Adds the value's count instead of 1, so partial sums work too (e.g. from a Combiner).
        for (SumCountWritable value : values) {
            sum += value.getSum();
            count += value.getCount();
        }

        if (count > 0) {
            context.write(key, new DoubleWritable(sum / count));
        }
    }
}
