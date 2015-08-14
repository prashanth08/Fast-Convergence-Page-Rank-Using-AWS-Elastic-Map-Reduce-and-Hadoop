package com.random.blocked;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.blocked.BlockPageRankMaster;
import com.common.Constants;
import com.common.Counter;

public class RandomBlockPageRankMaster {
	public static Map<String, String> additionalOutput = new HashMap<>();
	
	/**
	 * Nodes are assigned to Blocks in a random fashion using a hash function.
	 * The standard Blocked PageRank computation uses METIS graph blocking procedure.
	 */
	public static int blockIdOfNode(int nodeId) {
		return (nodeId * 541) % 68;
	}

	public static Job createJob(String input, String output) throws IOException {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "Random Block Page Rank");

		job.setJarByClass(RandomBlockPageRankMaster.class);

		job.setMapperClass(RandomBlockPageRankMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(com.blocked.BlockPageRankReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		return job;
	}

	public static void main(String args[]) throws Exception {
		int iteration = 1;

		StringBuilder output = new StringBuilder();

		while (iteration <= 10) {
			String inputPath = (iteration == 1) ? args[0] : args[1]
					+ (iteration - 1);
			String outputPath = args[1] + iteration;

			Job job = createJob(inputPath, outputPath);
			job.waitForCompletion(true);

			long longResidue = job.getCounters().findCounter(Counter.RESIDUAL)
					.getValue();
			double residue = longResidue / Constants.FACTOR;
			residue /= Constants.N;

			output.append("Iteration " + iteration + ", Avg Error: " + residue
					+ "\n");

			if (residue <= Constants.THRESHOLD)
				break;

			job.getCounters().findCounter(Counter.RESIDUAL).setValue(0);
			iteration++;
		}

		System.out.println("Done computing PageRanks (Random Blocked computation) for the given graph.");
		System.out.println("==========================================");
		System.out.print(output);
		System.out.println("==========================================");
		
		for(Entry<Integer, String> entry : BlockPageRankMaster.additionalOutput.entrySet()) {
			System.out.println("Block ID: "+entry.getKey()+", "+ entry.getValue());
		}
	}
}
