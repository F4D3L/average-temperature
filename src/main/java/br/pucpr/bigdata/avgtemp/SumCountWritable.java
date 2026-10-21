package br.pucpr.bigdata.avgtemp;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Composite value carried from Map to Reduce: a partial sum of temperatures and how many
 * readings that sum covers.
 *
 * <p>An average cannot be built by averaging partial averages, because each part may hold a
 * different number of readings (Lecture 05: 9.42 instead of 9.94). Carrying the pair
 * {@code (sum, count)} keeps every reading with the same weight, and the division happens only
 * once, in the reducer.
 *
 * <p>Hadoop serializes keys and values between the map and reduce phases, so the class implements
 * {@link Writable}: {@link #write} and {@link #readFields} must handle the fields in the same order.
 */
public class SumCountWritable implements Writable {

    private double sum;
    private long count;

    /** Hadoop creates values through reflection and then calls {@link #readFields}, so a no-arg constructor is required. */
    public SumCountWritable() {
    }

    public SumCountWritable(double sum, long count) {
        set(sum, count);
    }

    /** Lets a mapper reuse one instance instead of allocating a new object for every line. */
    public void set(double sum, long count) {
        this.sum = sum;
        this.count = count;
    }

    public double getSum() {
        return sum;
    }

    public long getCount() {
        return count;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(sum);
        out.writeLong(count);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        sum = in.readDouble();
        count = in.readLong();
    }

    /** Used when the value is written to a text output file. */
    @Override
    public String toString() {
        return sum + "\t" + count;
    }
}
