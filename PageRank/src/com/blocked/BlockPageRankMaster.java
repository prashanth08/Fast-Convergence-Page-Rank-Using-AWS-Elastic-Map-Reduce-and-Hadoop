package com.blocked;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.common.Constants;
import com.common.Counter;

public class BlockPageRankMaster {
	
	// Used in NodeID -> BlockID calculation.
	private static List<Integer> blockPrefixSums = new ArrayList<Integer>();
	
	// BlockID -> Output required to be printed (PageRank values of first 2 nodes in each block).
	public static Map<Integer, String> additionalOutput = new TreeMap<>();
	
	// Metric required to be tracked and printed.
	public static int iterationsPerBlock = 0;  
	
	/**
	 * Return the BlockId for a given NodeID.
	 * Nodes are divided into blocks using METIS.
	 */
	public static int blockIdOfNode(int nodeId) {
		int i = Collections.binarySearch(blockPrefixSums, nodeId);
		return i >= 0 ? i : -(i + 1);
	}

	private static void init() {
		try {
			List<String> lines = Files.readAllLines(
					Paths.get(new File("blocks.txt").getAbsolutePath()),
					Charset.defaultCharset());

			blockPrefixSums.add(Integer.parseInt(lines.get(0).trim()));
			for (int i = 1; i < lines.size(); i++) {
				blockPrefixSums.add(blockPrefixSums.get(i - 1)
						+ Integer.parseInt(lines.get(i).trim()));
			}
		} catch (Exception e) {
			System.out
					.println("Unable to initialise node blocks. Map-Reduce will fail.");
		}
	}

	public static Job createJob(String input, String output) throws IOException {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "Block Page Rank");

		job.setJarByClass(BlockPageRankMaster.class);

		job.setMapperClass(BlockPageRankMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(BlockPageRankReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		return job;
	}

	public static void main(String args[]) throws Exception {
		init();

		int iteration = 1;

		StringBuilder output = new StringBuilder();
		
		// iterate until convergence
		while (true) {
			String inputPath = (iteration == 1) ? args[0] : args[1]
					+ (iteration - 1);
			String outputPath = args[1] + iteration;

			Job job = createJob(inputPath, outputPath);
			job.waitForCompletion(true);

			// calculate average residue and use this as criteria for convergence
			long longResidue = job.getCounters().findCounter(Counter.RESIDUAL)
					.getValue();
			double residue = longResidue / Constants.FACTOR;
			residue /= Constants.N;

			String res = "Iteration " + iteration + ", Avg Error: " + residue
					+ "\n";
			
			System.out.println(res);
			
			double avgIterationsPerBlock = iterationsPerBlock / 68d;
			System.out.println("Avg iterations per block = "+avgIterationsPerBlock);
			
			output.append(res);			
			iterationsPerBlock = 0;
			
			if (residue <= Constants.THRESHOLD)
				break;

			job.getCounters().findCounter(Counter.RESIDUAL).setValue(0);
			iteration++;
		}

		System.out.println("Done computing PageRanks (Blocked computation) for the given graph.");
		System.out.println("==========================================");
		System.out.print(output);
		System.out.println("==========================================");
		
		for(Entry<Integer, String> entry : additionalOutput.entrySet()) {
			System.out.println("Block ID: "+entry.getKey()+", "+ entry.getValue());
		}
	}
}
