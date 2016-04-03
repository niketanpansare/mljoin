package mljoin;

public class GMMModel implements Model {
	private static final long serialVersionUID = -1953439650104092103L;
	double[] mean;
	double[] cov;
	
	public GMMModel(double[] mean, double[] cov) {
		this.mean = mean;
		this.cov = cov;
	}
	
	@Override
	public Model process() {
		// TODO: Jacob
		return this;
	}

}
