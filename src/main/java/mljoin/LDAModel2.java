package mljoin;

public class LDAModel2 implements Model2 {
	private static final long serialVersionUID = -1953439650104092103L;
	
	int wordBlockID;
	double topicProbs[][];
	
	public LDAModel2(int wordBlockID) {
		this.wordBlockID = wordBlockID;
		topicProbs = new double[LDAData2.T][LDAData2.WBS];	// the big matrix of T x WBS!
		for (int i = 0; i < LDAData2.T; i++)
			for (int j = 0; j < LDAData2.WBS; j++)
				topicProbs[i][j] = 1.0;		// initialize topicProbs with 1.0
	}
	
	public double[][] getTopicProbs() {
		return topicProbs;
	}
	
	public LDAModel2 setTopicRow(int topicID, double[] topicProb) {
		topicProbs[topicID] = topicProb;
		return this;
	}

	public Model2 process() {
		// TODO: Jacob
		// nothing to do here for LDA
		// Statistics.modelProcessTime().addAndGet(System.nanoTime()-start);
		return this;
	}
	
}
