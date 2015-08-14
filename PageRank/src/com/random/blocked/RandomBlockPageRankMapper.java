package com.random.blocked;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RandomBlockPageRankMapper extends
		Mapper<Object, Text, IntWritable, Text> {

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		String record[] = value.toString().trim().split(" ");
		String nodeID = record[0].trim();
		
		// nodes are assigned to blocks in a random fashion.
		int blockId = RandomBlockPageRankMaster.blockIdOfNode(Integer
				.parseInt(nodeID));
		
		double pageRank = Double.parseDouble(record[1]);
		int outlinks = Integer.parseInt(record[2]);

		String oldRecord = "PR " + value.toString().trim();
		context.write(new IntWritable(blockId), new Text(oldRecord));

		if (record.length > 3) {
			double outboundPageRank = pageRank / outlinks;

			for (int i = 3; i < record.length; i++) {
				String adjacentNode = record[i];

				int adjacentNodeBlockId = RandomBlockPageRankMaster
						.blockIdOfNode(Integer.parseInt(adjacentNode));

				if (blockId == adjacentNodeBlockId) {
					String mapRecord = "BE " + nodeID + " " + adjacentNode;
					context.write(new IntWritable(adjacentNodeBlockId),
							new Text(mapRecord));
				} else {
					String mapRecord = "BC " + nodeID + " " + adjacentNode
							+ " " + outboundPageRank;
					context.write(new IntWritable(adjacentNodeBlockId),
							new Text(mapRecord));
				}
			}
		}
	}
}
