package mljoin;

import java.util.ArrayList;

public class LDAOutput2 implements Output2 {
	private static final long serialVersionUID = -6887772726150300326L;

	private ArrayList<LDADelta2> tuples;
	
	public LDAOutput2() {
		tuples = new ArrayList<>();
	}
	
	public void addTuple(LDADelta2 tuple) {
		tuples.add(tuple);
	}
	
	public ArrayList<LDADelta2> getTuples() {
		return tuples;
	}
	
}
