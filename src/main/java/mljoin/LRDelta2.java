package mljoin;

import java.util.ArrayList;
import scala.collection.Iterable;
import scala.collection.*;

public class LRDelta2 implements Delta2 {
private static final long serialVersionUID = 1224035511869097599L;
	
	double[] gradient;
	
	public LRDelta2(double[] gradient) {
		this.gradient = gradient;
	}
	
	public double[] getGradient() {
		return gradient;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Iterable<Delta2> combine(Iterable<Delta2> d1, Iterable<Delta2> d2) {
		double [] sum = new double[gradient.length];
		ArrayList<Delta2> ret = new ArrayList<>();
		ret.add(new LRDelta2(sum));
		for(Delta2 d : JavaConversions.asJavaIterable(d1)) {
			if(d instanceof LRDelta2) {
				double [] g = ((LRDelta2)d).gradient;
				for(int i = 0; i < sum.length; i++) {
					sum[i] += g[i];
				}
			}
			else {
				throw new RuntimeException("Cannot combine deltas of two different types");
			}
		}
		for(Delta2 d : JavaConversions.asJavaIterable(d2)) {
			if(d instanceof LRDelta2) {
				double [] g = ((LRDelta2)d).gradient;
				for(int i = 0; i < sum.length; i++) {
					sum[i] += g[i];
				}
			}
			else {
				throw new RuntimeException("Cannot combine deltas of two different types");
			}
		}
		return (Iterable<Delta2>) ret;
	}
}
