package mljoin;

import java.io.Serializable;

public class LDADelta2 implements Delta2, Serializable {
	private static final long serialVersionUID = 5605710211026872436L;
	
	int topicID;
	int wordID;
	int count;
	
	public LDADelta2(int topicID, int wordID, int count) {
		this.topicID = topicID;
		this.wordID = wordID;
		this.count = count;
	}
	
	public int getTopicID() {
		return topicID;
	}

	public int getWordID() {
		return wordID;
	}
	
	public int getCount() {
		return count;
	}
}
