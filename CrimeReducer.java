// CrimeReducer.java

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CrimeReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable totalCount = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;

        // Sum up the counts
        for (IntWritable value : values) {
            sum += value.get();
        }

        // Set the total count
        totalCount.set(sum);

        // Output the result
        context.write(key, totalCount);
    }
}
