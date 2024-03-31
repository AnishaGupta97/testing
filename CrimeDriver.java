// CrimeDriver.java

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CrimeDriver {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: CrimeDriver <input path> <output path>");
            System.exit(-1);
        }

        // Create a new MapReduce job
        Job job = Job.getInstance();
        job.setJarByClass(CrimeDriver.class);
        job.setJobName("Aggravated Assaults and Robberies Count");

        // Set input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Set Mapper and Reducer classes
        job.setMapperClass(CrimeMapper.class);
        job.setReducerClass(CrimeReducer.class);

        // Set key and value types for the output
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Wait for the job to complete and print status
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
