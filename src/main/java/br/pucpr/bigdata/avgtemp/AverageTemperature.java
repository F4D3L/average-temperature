package br.pucpr.bigdata.avgtemp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Driver (main class): configures and submits the MapReduce job that computes the average
 * temperature of {@code forestfireinput.csv}.
 *
 * <pre>
 * Map:     line             -> ("temperature", (temp, 1))
 * Shuffle: groups by key    -> ("temperature", [(t1, 1), (t2, 1), ...])
 * Reduce:  sum(t) / sum(1)  -> ("temperature", average)
 * </pre>
 *
 * <p>Usage: {@code hadoop jar average-temperature-1.0.0.jar <input path> <output dir>}. The output
 * directory must not exist yet; Hadoop refuses to overwrite it.
 */
public class AverageTemperature {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: AverageTemperature <input path> <output dir>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "average temperature");
        // Tells Hadoop which jar to ship to the nodes that run the tasks.
        job.setJarByClass(AverageTemperature.class);

        job.setMapperClass(TemperatureMapper.class);
        job.setReducerClass(TemperatureReducer.class);

        // The map output types differ from the final output types, so both pairs are declared.
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SumCountWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
