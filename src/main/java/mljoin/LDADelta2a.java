package mljoin;

import java.io.Serializable;

import scala.collection.Iterable;

// Memory efficient version of LDADelta2
public class LDADelta2a implements Delta2, Serializable {
	private static final long serialVersionUID = 5900291380089509662L;
	public int [] topicIDs;
	public int [] wordIDs;
	@Override
	public Iterable<Delta2> combine(Iterable<Delta2> d1, Iterable<Delta2> d2) {
		throw new RuntimeException("TODO: Not implemented");
	}
}
