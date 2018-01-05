package cs.bigdata.Lab2.preprocessing;
import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class PreprocessingMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

@Override
protected void map(LongWritable keyE, Text valE, Context context) throws IOException,InterruptedException
    {

		String line = valE.toString();
		
		try {
			// Split line with tab
			String[] splittedLine = line.split("	");
			
			// Get nodeId and outlinkId info
			int nodeId = Integer.parseInt(splittedLine[0]);
			int outlinkId = Integer.parseInt(splittedLine[1]);
			
			// Emit (nodeId, outlinkId)
			context.write(new IntWritable(nodeId), new IntWritable(outlinkId));
		}
		catch(Exception e) {
			System.out.println(e.toString());
		}
    }

public void run(Context context) throws IOException, InterruptedException {
    setup(context);
    while (context.nextKeyValue()) {
        map(context.getCurrentKey(), context.getCurrentValue(), context);
    }
    cleanup(context);
}

}






