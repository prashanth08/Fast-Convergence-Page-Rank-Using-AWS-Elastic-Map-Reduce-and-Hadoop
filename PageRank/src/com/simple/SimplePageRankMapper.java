package com.simple;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SimplePageRankMapper extends
		Mapper<Object, Text, IntWritable, Text> {

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		String record[] = value.toString().trim().split(" ");
		IntWritable source = new IntWritable(Integer.parseInt(record[0].trim()));
		double pageRank = Double.parseDouble(record[1]);
		int outLinks = Integer.parseInt(record[2]);

		String oldRecord = "PR_" + value.toString().trim();
		context.write(source, new Text(oldRecord));

		if (record.length > 3) {
			double outBoundPageRank = pageRank / outLinks;

			for (int i = 3; i < record.length; i++) {
				// emit node and its outbound page_rank 
				context.write(new IntWritable(Integer.parseInt(record[i])),
						new Text(String.valueOf(outBoundPageRank)));
			}
		}
	}
}
