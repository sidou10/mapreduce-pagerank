package cs.bigdata.Lab2.preprocessing;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import cs.bigdata.Lab2.utils.Vertex;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;


public class PreprocessingReducer extends Reducer<IntWritable, IntWritable, IntWritable, Vertex> {

    private final static int nbNodes = 75879;
    
    @Override
    public void reduce(final IntWritable key, final Iterable<IntWritable> values,
            final Context context) throws IOException, InterruptedException {
    		
        Iterator<IntWritable> iterator = values.iterator();
        // For a given nodeId, we will initialize the page rank to 1/nbNodes
        // And compute its adjacencyList (all the nodes to which it points)
        // These 2 info are gathered in the Vertex object
        
        ArrayList<Integer> adjacencyList = new ArrayList<Integer>();
        while (iterator.hasNext()) {
        		adjacencyList.add(iterator.next().get());
        }
        
		Vertex v = new Vertex(new DoubleWritable(1.0/nbNodes), new Text(adjacencyList.toString()));
        
		// Emit (nodeId, Vertex(initialPageRank, adjacencyList))
		context.write(key, v);
    }
}
