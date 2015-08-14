package com.simple;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.StringUtils;

import com.common.Constants;
import com.common.Counter;

public class SimplePageRankReducer extends
		Reducer<IntWritable, Text, Text, Text> {

	public void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		double oldPageRank = 0, newPageRank = 0;
		String[] adjList = null;
		int outLinks = 0;
		
		// iterate over all page_rank values received for a given node and calculate its new page_rank
		for (Text text : values) {
			String value = text.toString();

			if (value.contains("PR_")) {
				String[] record = value.split(" ");
				oldPageRank = Double.parseDouble(record[1]); 
				
				if (record.length > 3){
					adjList = Arrays.copyOfRange(record, 3, record.length);
					outLinks = adjList.length;
				}
				else{
					outLinks = 0;
				}
			} 
			else {
				newPageRank += Double.parseDouble(value);
			}
		}

		newPageRank *= Constants.d;
		newPageRank += (1 - Constants.d) / Constants.N;
				
		String outputRecord = key.toString() + " " + newPageRank +" "+ outLinks;
		if (adjList != null)
			outputRecord += " " + StringUtils.join(" ", adjList);
		
		// emit node with new page_rank. this will serve as input for the next Map-Reduce pass.
		context.write(new Text(""), new Text(outputRecord));
		
		// set the residual counter
		double residue = Math.abs(newPageRank - oldPageRank) / newPageRank;
		long residue_To_Long = (long) (Constants.FACTOR * residue);
		
		context.getCounter(Counter.RESIDUAL).increment(residue_To_Long);
	}
}
