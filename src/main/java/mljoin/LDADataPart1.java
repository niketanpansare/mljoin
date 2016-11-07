package mljoin;

public class LDADataPart1 implements DataPart1 {
	
	private static final long serialVersionUID = -8280023097269439961L;
	
	int wordsInDoc[];
	int wordCounts[];
	int wordBlockID;
	
	public LDADataPart1(int wordBlockID, int[] wordsInDoc, int[] wordCounts) {
		this.wordBlockID = wordBlockID;
		this.wordsInDoc = wordsInDoc;
		this.wordCounts = wordCounts;
	}

	public int[] getWordsInDoc() {
		return wordsInDoc;
	}

	public int[] getWordCounts() {
		return wordCounts;
	}

	public int getWordBlockID() {
		return wordBlockID;
	}
	
}
