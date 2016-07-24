package mljoin;

import java.util.ArrayList;

public class LROutput2 implements Output2 {
	private static final long serialVersionUID = -6887772726150300326L;

	private ArrayList<LRDelta2> tuples;
	
	public LROutput2() {
		tuples = new ArrayList<>();
	}
	
	public void addTuple(LRDelta2 tuple) {
		tuples.add(tuple);
	}
	
	public ArrayList<LRDelta2> getTuples() {
		return tuples;
	}
}
