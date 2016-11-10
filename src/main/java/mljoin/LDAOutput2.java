package mljoin;

import java.util.ArrayList;

public class LDAOutput2 implements Output2 {
	private static final long serialVersionUID = -6887772726150300326L;

	private ArrayList<LDADelta2a> tuples;
	
	public LDAOutput2() {
		tuples = new ArrayList<>();
	}
	
	public void addTuple(LDADelta2a tuple) {
		tuples.add(tuple);
	}
	
	public ArrayList<LDADelta2a> getTuples() {
		return tuples;
	}
	
}
