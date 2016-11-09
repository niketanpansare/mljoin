package mljoin;

import java.io.Serializable;

// Memory efficient version of LDADelta2
public class LDADelta2a implements Delta2, Serializable {
	private static final long serialVersionUID = 5900291380089509662L;
	public int [] topicIDs;
	public int [] wordIDs;
}
