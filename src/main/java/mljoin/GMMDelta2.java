package mljoin;

import scala.collection.Iterable;

public class GMMDelta2 implements Delta2 {
	private static final long serialVersionUID = 1224035511869097599L;
	
	double[] data;
	int clusterID;
	
	public GMMDelta2(double[] data) {
		this.data = data;
	}
	
	public double[] getData() {
		return data;
	}

	public int getClusterID() {
		return clusterID;
	}

	public GMMDelta2 setClusterID(int clusterID) {
		this.clusterID = clusterID;
		return this;
	}

	@Override
	public Iterable<Delta2> combine(Iterable<Delta2> d1, Iterable<Delta2> d2) {
		throw new RuntimeException("TODO: Not implemented");
	}
	
}
