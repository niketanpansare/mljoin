package mljoin;

import java.util.ArrayList;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SQLContext;

public class LRData2 implements Data2 {
	public static double rate = 0.1; 	// learning rate
	public static int N = 10;		// number of data points
	public static int D = 4;			// number of dimensions
	
	private static final long serialVersionUID = -8280023097269439963L;
	
	int id;
	double x[];
    double y;
	
	public LRData2(int id, double[] x, double y) {
		this.id = id;
		this.x = x;
		this.y = y;
	}

	public ArrayList<Delta2> process(Model2 m) {
		// TODO: Jacob
		double[] gradient = calculateGradient(x, y, ((LRModel2)m).getTheta());
		ArrayList<Delta2> tuples = new ArrayList<Delta2>();
		tuples.add(new LRDelta2(gradient));
		return tuples;
	}
	
	public double[] calculateGradient(double[] x, double y, double[] theta) {
		double factor = 0.0;
		for (int i = 0; i < x.length; i++) {
			factor += theta[i] * x[i];
		}
		factor -= y;
		double[] gradient = new double[x.length];
		for (int i = 0; i < x.length; i++) {
			gradient[i] = factor * x[i];
		}
		return gradient;
	}
	
}
