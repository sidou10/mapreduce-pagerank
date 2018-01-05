package cs.bigdata.Lab2.postprocessing;
import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class PostprocessingMapper extends Mapper<LongWritable, Text, DoubleWritable, IntWritable> {
	
@Override
protected void map(LongWritable keyE, Text valE, Context context) throws IOException,InterruptedException
    {
		String line = valE.toString();
		
		String[] keyValue = line.split("	");
		String[] valueSplitted = keyValue[1].split(" / ");
		
		double pageRank = Double.parseDouble(valueSplitted[0]);
		int nodeId = Integer.parseInt(keyValue[0]);
		
		// Emit (pageRank, nodeId)
		// This step aims to sort key by pageRank in an ascending fashion
		// The highest pageRanks will be at the end of the file
		context.write(new DoubleWritable(pageRank), new IntWritable(nodeId));
    }

public void run(Context context) throws IOException, InterruptedException {
    setup(context);
    while (context.nextKeyValue()) {
        map(context.getCurrentKey(), context.getCurrentValue(), context);
    }
    cleanup(context);
}

}






