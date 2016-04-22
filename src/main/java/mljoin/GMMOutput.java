package mljoin;

public class GMMOutput implements Output {
	private static final long serialVersionUID = -6887772726150300326L;

	double[] data;
	int clusterID;
	
	public GMMOutput(double[] data) {
		this.data = data;
	}
	
	public double[] getData() {
		return data;
	}

	public int getClusterID() {
		return clusterID;
	}

	public GMMOutput setClusterID(int clusterID) {
		this.clusterID = clusterID;
		return this;
	}
	
}
