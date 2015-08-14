package com.simple;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.common.Constants;
import com.common.Counter;

/**
 * Map-Reduce Master for Simple (Node-by-node) PageRank Computation.
 * Setup Map and Reduce jobs
 * Setup required Counters
 * Setup input & output paths
 * 
 * @author kvb26
 */
public class SimplePageRankMaster {

	public static Job createJob(String input, String output) throws IOException {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "Simple Page Rank");

		job.setJarByClass(SimplePageRankMaster.class);

		job.setMapperClass(SimplePageRankMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(SimplePageRankReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		return job;
	}

	public static void main(String args[]) throws Exception {
		int iteration = 1;
		
		StringBuilder output = new StringBuilder();
		
		// run until convergence
		while (true) {
			String inputPath = (iteration == 1) ? args[0] : args[1]
					+ (iteration - 1);
			String outputPath = args[1] + iteration;

			Job job = createJob(inputPath, outputPath);
			job.waitForCompletion(true);

			long longResidue = job.getCounters().findCounter(Counter.RESIDUAL)
					.getValue();
			double residue = longResidue / Constants.FACTOR;
			residue /= Constants.N;
			
			output.append("Iteration "+ iteration + ", Avg Error: " + residue + "\n");
			
			if (residue <= Constants.THRESHOLD)
				break;
			
			// reset residual counter after each iteration
			job.getCounters().findCounter(Counter.RESIDUAL).setValue(0);
			iteration++;
		}
		
		System.out.println("Done computing PageRanks (Node-by-node computation) for the given graph.");
		System.out.println("==========================================");
		System.out.print(output);
		System.out.println("==========================================");
	}
}
