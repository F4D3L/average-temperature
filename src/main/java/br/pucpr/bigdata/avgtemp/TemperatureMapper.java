package br.pucpr.bigdata.avgtemp;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Map phase: reads one CSV line and emits {@code ("temperature", (temp, 1))}.
 *
 * <p>Input key/value: byte offset of the line in the file / the line itself (from
 * {@code TextInputFormat}). Output key/value: {@link Text} / {@link SumCountWritable}.
 *
 * <p>Every reading is emitted under the same <b>global key</b>. Sort/shuffle groups values by key,
 * so a single key sends all readings to the same reducer, which is what a global average needs.
 *
 * <p>The file has no header row. Columns: X, Y, month, day, FFMC, DMC, DC, ISI, temp, RH, wind,
 * rain, area, so the temperature is at index 8 (0-based).
 */
public class TemperatureMapper extends Mapper<LongWritable, Text, Text, SumCountWritable> {

    static final int TEMPERATURE_INDEX = 8;

    /** Hadoop counters, shown in the job summary, for lines that could not be used. */
    enum Readings {
        MALFORMED
    }

    private static final Text GLOBAL_KEY = new Text("temperature");

    // Reused across calls: context.write serializes the value right away, so this is safe.
    private final SumCountWritable reading = new SumCountWritable();

    @Override
    protected void map(LongWritable offset, Text line, Context context)
            throws IOException, InterruptedException {
        String[] fields = line.toString().split(",");
        if (fields.length <= TEMPERATURE_INDEX) {
            context.getCounter(Readings.MALFORMED).increment(1);
            return;
        }

        double temperature;
        try {
            temperature = Double.parseDouble(fields[TEMPERATURE_INDEX].trim());
        } catch (NumberFormatException e) {
            // A header or a corrupted line: skip it instead of failing the whole job.
            context.getCounter(Readings.MALFORMED).increment(1);
            return;
        }

        // Composite value: this single reading is a sum of `temperature` over 1 reading.
        reading.set(temperature, 1);
        context.write(GLOBAL_KEY, reading);
    }
}
