package mljoin;

public class LDADataPart2 implements DataPart2 {
	
	private static final long serialVersionUID = -8280023097269439962L;
	
	public static int T = 1250;		// total number of topics

	double docProbs[];
	
	public LDADataPart2() {
		docProbs = new double[T];
		StatUtils.dirichlet(docProbs, 1.0);
	}

	public double[] getDocProbs() {
		return docProbs;
	}

	public void setDocProbs(double[] docProbs) {
		this.docProbs = docProbs;
	}
	
}
