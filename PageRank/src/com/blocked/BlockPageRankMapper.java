package com.blocked;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BlockPageRankMapper extends
		Mapper<Object, Text, IntWritable, Text> {

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		// System.out.println("Mapper Input: "+value.toString().trim());

		String record[] = value.toString().trim().split(" ");
		String nodeID = record[0].trim();
		int blockId = BlockPageRankMaster.blockIdOfNode(Integer
				.parseInt(nodeID));
		double pageRank = Double.parseDouble(record[1]);
		int outlinks = Integer.parseInt(record[2]);

		String oldRecord = "PR " + value.toString().trim();
		context.write(new IntWritable(blockId), new Text(oldRecord));

		// System.out.println("Mapper Output: "+oldRecord);

		if (record.length > 3) {
			double outboundPageRank = pageRank / outlinks;

			for (int i = 3; i < record.length; i++) {
				String adjacentNode = record[i];

				int adjacentNodeBlockId = BlockPageRankMaster
						.blockIdOfNode(Integer.parseInt(adjacentNode));

				if (blockId == adjacentNodeBlockId) {
					String mapRecord = "BE " + nodeID + " " + adjacentNode;
					// System.out.println("Mapper Output: "+mapRecord);
					context.write(new IntWritable(adjacentNodeBlockId),
							new Text(mapRecord));
				} else {
					String mapRecord = "BC " + nodeID + " " + adjacentNode
							+ " " + outboundPageRank;
					context.write(new IntWritable(adjacentNodeBlockId),
							new Text(mapRecord));
					// System.out.println("Mapper Output: "+mapRecord);
				}
			}
		}
	}
}
