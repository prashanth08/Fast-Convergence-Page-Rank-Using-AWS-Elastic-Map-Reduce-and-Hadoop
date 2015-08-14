package com.blocked;

/**
 * Represents a Node in the given graph 
 * @author kvb26
 */
public class Node {
	private final String id;
	private double curPageRank;
	private String[] adjacentNodes;
	
	public Node(String id) {
		this.id = id;
	}
	
	public String getId() {
		return id;
	}

	public String[] getAdjacentNodes() {
		return adjacentNodes;
	}

	public void setAdjacentNodes(String[] adjacentNodes) {
		this.adjacentNodes = adjacentNodes;
	}
	
	public int getOutDegree() {
		return adjacentNodes != null ? adjacentNodes.length : 0; 
	}
	
	public double getCurPageRank() {
		return curPageRank;
	}

	public void setCurPageRank(double curPageRank) {
		this.curPageRank = curPageRank;
	}
}
