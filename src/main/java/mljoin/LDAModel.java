package mljoin;

public class LDAModel implements Model {
	private static final long serialVersionUID = -1953439650104092103L;
	
	int wordBlockID;
	double topicProbs[][];
	
	public LDAModel(int wordBlockID) {
		this.wordBlockID = wordBlockID;
		topicProbs = new double[LDAData.T][LDAData.WBS];	// the big matrix of T x WBS!
		for (int i = 0; i < LDAData.T; i++)
			for (int j = 0; j < LDAData.WBS; j++)
				topicProbs[i][j] = 1.0;		// initialize topicProbs with 1.0
	}
	
	public double[][] getTopicProbs() {
		return topicProbs;
	}
	
	public LDAModel setTopicRow(int topicID, double[] topicProb) {
		topicProbs[topicID] = topicProb;
		return this;
	}

	@Override
	public Model process() {
		// TODO: Jacob
		// nothing to do here for LDA
		return this;
	}
	
}
