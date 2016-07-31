package mljoin;

public class LRModel2 implements Model2 {
	private static final long serialVersionUID = -1953439650104092103L;
	
	double theta[];
	
	public LRModel2() {
		theta = new double[LRData2.D];
	}
	
	public double[] getTheta() {
		return theta;
	}

	public Model2 process() {
		// TODO: Jacob
		// nothing to do here for LR
		// Statistics.modelProcessTime().addAndGet(System.nanoTime()-start);
		return this;
	}
	
}
