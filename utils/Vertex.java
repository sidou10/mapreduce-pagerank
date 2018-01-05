package cs.bigdata.Lab2.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class Vertex implements Writable{
	
	private DoubleWritable pageRank;
	private Text adjacencyList;
	
	public Vertex() {
		super();
		pageRank = new DoubleWritable();
		adjacencyList = new Text();
	}

	public Vertex(DoubleWritable pageRank, Text adjacencyList) {
		super();
		this.pageRank = pageRank;
		this.adjacencyList = adjacencyList;
	}
	
	public Vertex(DoubleWritable pageRank) {
		super();
		this.pageRank = pageRank;
		this.adjacencyList = new Text();
	}

	public DoubleWritable getPageRank() {
		return pageRank;
	}

	public void setPageRank(DoubleWritable pageRank) {
		this.pageRank = pageRank;
	}


	public Text getAdjacencyList() {
		return adjacencyList;
	}


	public void setAdjacencyList(Text adjacencyList) {
		this.adjacencyList = adjacencyList;
	}


	public Vertex(double pageRank, Text adjacencyList) {
		super();
		this.pageRank = new DoubleWritable(pageRank);
		this.adjacencyList = adjacencyList;
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		pageRank.readFields(arg0);
		adjacencyList.readFields(arg0);
		
	}
	
	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		pageRank.write(arg0);
		adjacencyList.write(arg0);
	}
	
	public String toString() {
		return pageRank.toString() + " / " + adjacencyList.toString();
	}
}
