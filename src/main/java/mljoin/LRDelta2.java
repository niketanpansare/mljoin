package mljoin;

public class LRDelta2 implements Delta2 {
private static final long serialVersionUID = 1224035511869097599L;
	
	double[] gradient;
	
	public LRDelta2(double[] gradient) {
		this.gradient = gradient;
	}
	
	public double[] getGradient() {
		return gradient;
	}
}
