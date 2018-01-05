package cs.bigdata.Lab2.iterations;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import cs.bigdata.Lab2.utils.Vertex;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.IntWritable;

public class PageRankIterReducer extends Reducer<IntWritable, Vertex, IntWritable, Vertex> {

    private final static double dampingFactor = 0.85;
    private final static int nbNodes = 75879;
    
    @Override
    public void reduce(final IntWritable key, final Iterable<Vertex> values,
            final Context context) throws IOException, InterruptedException {
    	    		
    			
    			Vertex vertexToEmit = new Vertex();
    			ArrayList<Integer> adjacencyList = new ArrayList<Integer>();
    			
    	    		double contributions = 0;
	    		for (Vertex v: values) {
	    			// If the adjacencyList is not empty, then it is the first type of emit
	    			// All we do is get the adjacencyList
	    			if (!(v.getAdjacencyList().equals(new Text()))) {
	    				for(String adjacent: v.getAdjacencyList().toString().split(", ")) {
	    					try {
		    					int adjacentProcessed = Integer.parseInt(adjacent.replace("[", "").replace("]", ""));
		    					adjacencyList.add(adjacentProcessed);
	    					}
	    					catch(Exception e) {
	    						e.toString();
	    					}
	    				}
	    			}
	    			// If the adjacencyList is empty, then it is the second type of emit
	    			// And we add all the contributions to this node
	    			else{
	    				contributions += v.getPageRank().get();
	    			}
	    		}
	    		
	    		// Compute new page rank
	    		double newPageRank = contributions*dampingFactor + (1-dampingFactor)/nbNodes;
	    		
	    		// Update vertexToEmit with info
	    		vertexToEmit.setPageRank(new DoubleWritable(newPageRank));
	    		vertexToEmit.setAdjacencyList(new Text(adjacencyList.toString()));
	    		
	    		// Emit (nodeId, Vertex(newPageRank, adjacencyList))
	    		context.write(key, vertexToEmit);
    		
    }
}
