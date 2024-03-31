// CrimeMapper.java

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CrimeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private static final IntWritable ONE = new IntWritable(1);
    private Text crimeType = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Split the line into columns
        String[] columns = value.toString().split("\t");

        // Extract the offense type (fifth column)
        String offenseType = columns[4];

        // Check if the offense type is "Aggravated Assault" or "Robbery"
        if (offenseType.equals("Aggravated Assault") || offenseType.equals("Robbery")) {
            crimeType.set(offenseType);
            // Output key-value pair
            context.write(crimeType, ONE);
        }
    }
}

