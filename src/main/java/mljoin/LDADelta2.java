package mljoin;

import java.io.Serializable;

public class LDADelta2 implements Delta2, Serializable {
	private static final long serialVersionUID = 5605710211026872436L;
	
	int topicID;
	int wordID;
	
	public LDADelta2(int topicID, int wordID) {
		this.topicID = topicID;
		this.wordID = wordID;
	}
	
	public int getTopicID() {
		return topicID;
	}

	public int getWordID() {
		return wordID;
	}
}
