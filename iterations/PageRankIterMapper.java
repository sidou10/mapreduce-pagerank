package cs.bigdata.Lab2.iterations; 
import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;
import cs.bigdata.Lab2.utils.Vertex;
import java.io.IOException;

public class PageRankIterMapper extends Mapper<LongWritable, Text, IntWritable, Vertex> {
	
@Override
protected void map(LongWritable keyE, Text valE, Context context) throws IOException,InterruptedException
    {
		String line = valE.toString();
		
		// Line format: nodeId currentPageRank / adjacencyList
		// Example: 75885	1.9768315344166374E-6 / [7900, 16086]
		
		// Get the needed information
		String[] keyValue = line.split("	");
		String[] valueSplitted = keyValue[1].split(" / ");
		
		String adjacencyList = valueSplitted[1];
		String[] adjacencyArray = adjacencyList.split(", ");
		
		int key = Integer.parseInt(keyValue[0]);
		// The vertex object associated to a nodeIt, contains it currentPageRank and its adjacencyList
		Vertex v = new Vertex(Double.parseDouble(valueSplitted[0]), new Text(adjacencyList));
		
		// Emit info of line (the objective is to keep the info of the adjacency list)
		context.write(new IntWritable(key), v);
		
		// p is the fraction of the rank that we will distribute to each of our neighbor
		double p = v.getPageRank().get()/adjacencyArray.length;
		
		for(String id: adjacencyArray) {
			try {
				// Get the ids of the adjacencyList
				int processedId = Integer.parseInt(id.replace("[", "").replace("]", ""));
				
				// Emit (adjacentNodeId, p)
				// We have to emit a Vertex because that is the type the reducer is waiting for
				// We just use it to pass the info "p"
				// In the reducer, we distinguish the case of empty adjacencyList (this emit)
				// vs. the case of non-empty adjacencyList (previous  emit)
				context.write(new IntWritable(processedId), new Vertex(new DoubleWritable(p)));	
			}
			catch(Exception e) {
				// For example, when there are no neighbors
				System.out.println(e.toString());
			}
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






