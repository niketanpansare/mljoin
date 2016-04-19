package mljoin;

import java.io.Serializable;
import java.util.ArrayList;

public class LDAOutput implements Output {
	private static final long serialVersionUID = -6887772726150300326L;

	private ArrayList<LDATuple> tuples;
	
	public LDAOutput() {
		tuples = new ArrayList<>();
	}
	
	public void addTuple(int topicID, int wordID, int count) {
		tuples.add(new LDATuple(topicID, wordID, count));
	}
	
	public ArrayList<LDATuple> getTuples() {
		return tuples;
	}

	public class LDATuple implements Serializable {
		private static final long serialVersionUID = 5605710211026872436L;
		
		int topicID;
		int wordID;
		int count;
		
		public LDATuple(int topicID, int wordID, int count) {
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
	
}
