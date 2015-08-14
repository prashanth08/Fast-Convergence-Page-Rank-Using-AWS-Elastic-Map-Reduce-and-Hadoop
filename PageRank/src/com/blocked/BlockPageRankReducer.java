package com.blocked;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.StringUtils;

import com.common.Constants;
import com.common.Counter;

public class BlockPageRankReducer extends
		Reducer<IntWritable, Text, Text, Text> {
	
	// Mapping from Node v to all inbound nodes belonging to the same block as v
	private Map<String, ArrayList<String>> BE = new HashMap<String, ArrayList<String>>();
	
	// Mapping from Node v to inbound PageRank contribution of a node belonging to a different block
	private Map<String, Double> BC = new HashMap<String, Double>();
	
	// Sorted Map of NodeID to Node
	private Map<String, Node> nodeMap = new TreeMap<String, Node>();

	public void reduce(IntWritable blockId, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {

		BE.clear();
		BC.clear();
		nodeMap.clear();

		for (Text value : values) {
			String input = value.toString().trim();
			String record[] = input.split(" ");

			if (record[0].equals("PR")) {
				String nodeId = record[1];
				Node node = new Node(nodeId);

				if (record.length > 4) {
					node.setAdjacentNodes(Arrays.copyOfRange(record, 4,
							record.length));
				}

				node.setCurPageRank(Double.parseDouble(record[2]));
				nodeMap.put(nodeId, node);

			} else if (record[0].equals("BE")) {
				String u = record[1];
				String v = record[2];

				ArrayList<String> uList = BE.get(v);
				if (uList == null) {
					uList = new ArrayList<String>();
					BE.put(v, uList);
				}

				uList.add(u);

			} else if (record[0].equals("BC")) {
				String v = record[2];
				double inboundPageRank = Double.parseDouble(record[3]);

				Double rank = BC.get(v);
				if (rank != null)
					rank += inboundPageRank;
				else
					rank = inboundPageRank;

				BC.put(v, rank);
			}
		}
		
		// Keep track of current page_rank of nodes before running inner iteration of blocked computation.
		// This will be required to calculate residuals for the inner iteration.
		Map<String, Double> startPageRanks = new HashMap<>();
		for (Node node : nodeMap.values())
			startPageRanks.put(node.getId(), node.getCurPageRank());

		// run inner iteration until convergence
		int iteration = 0;
		double residue = Double.MAX_VALUE;
		while (residue > Constants.THRESHOLD) {
			residue = pageRankOnBlock();
			iteration++;
		}

		BlockPageRankMaster.iterationsPerBlock += iteration;

		// Emit NewPageRank. This will be used as input for next Map-Reduce pass.
		// Also, calculate and set the residual counter used to determine convergence.
		residue = 0.0;
		for (Node node : nodeMap.values()) {
			String output = node.getId() + " " + node.getCurPageRank() + " "
					+ node.getOutDegree();

			if (node.getAdjacentNodes() != null)
				output += " " + StringUtils.join(" ", node.getAdjacentNodes());

			Text outputText = new Text(output);
			context.write(new Text(""), outputText);

			residue += Math.abs(startPageRanks.get(node.getId())
					- node.getCurPageRank())
					/ node.getCurPageRank();
		}
		
		// Track metrics required to be output at the end of the Map-Reduce computation.
		Iterator<Node> it = nodeMap.values().iterator();

		Node n1 = it.next();
		Node n2 = it.next();

		int key = blockId.get();
		String value = "PageRank of Node: " + n1.getId() + " is "
				+ n1.getCurPageRank() + ";";
		value += "PageRank of Node: " + n2.getId() + " is "
				+ n2.getCurPageRank() + ";";

		BlockPageRankMaster.additionalOutput.put(key, value);

		long residue_To_Long = (long) (Constants.FACTOR * residue);
		context.getCounter(Counter.RESIDUAL).increment(residue_To_Long);
	}
	
	/**
	 * Execution on each inner iteration of the Blocked Map
	 */
	private double pageRankOnBlock() {
		double residue = 0.0;

		HashMap<String, Double> npr = new HashMap<String, Double>();

		for (Node node : nodeMap.values()) {
			double newPageRank = 0.0;
			
			// Add inbound PageRank contribution of nodes in the same block. 
			ArrayList<String> uList = BE.get(node.getId());
			if (uList != null) {
				for (String u : uList) {
					Node uNode = nodeMap.get(u);

					// Jacobi: use page_ranks from last iteration
					newPageRank += uNode.getCurPageRank()
							/ uNode.getOutDegree();
				}
			}

			// Add inbound PageRank contribution of nodes from other blocks.
			Double inboundRank = BC.get(node.getId());
			if (inboundRank != null)
				newPageRank += inboundRank;

			newPageRank *= Constants.d;
			newPageRank += (1 - Constants.d) / Constants.N;

			residue += Math.abs(newPageRank - node.getCurPageRank())
					/ newPageRank;

			npr.put(node.getId(), newPageRank);
			
			// Uncomment the following line for Gauss-Siedel Implementation
			// node.setCurPageRank(newPageRank);
		}

		// Comment the follwing code for Gauss-Siedel implementation
		for (Node node : nodeMap.values()) {
			node.setCurPageRank(npr.get(node.getId()));
		}

		return residue / nodeMap.size();
	}
}
