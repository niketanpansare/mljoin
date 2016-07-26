package mljoin;

import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.linear.*;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.rdd.RDD;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class GMMData2 implements Data2 {
	public static int D = 2; 		// total number of dimensions
	public static int C = 3;		// total number of clusters
	
	private static final long serialVersionUID = -8280023097269439963L;
	
	int membership;
	double[] data;
	boolean[] missing;
	
	public GMMData2(int membership, double[] data) {
		this.membership = membership;
		this.data = data;
		missing = new boolean[D];
		for (int i = 0; i < D; i++) {
			if (data[i] == 0.0)
				missing[i] = true;
		}
	}

	public ArrayList<Delta2> process(Model2 m) {
		// TODO: Jacob
		long start = System.nanoTime();
		double[] mean = ((GMMModel2)m).getMean();
		double[][] cov = ((GMMModel2)m).getCov();
		double[][] invCov = ((GMMModel2)m).getInvCov();
		// impute data based on parameters above under initial membership
		imputeMissingData(data, missing, mean, cov, invCov);
		ArrayList<Delta2> tuples = new ArrayList<Delta2>();
		tuples.add(new GMMDelta2(data));
		Statistics.dataProcessTime().addAndGet(System.nanoTime()-start);
		return tuples;
	}
	
	// actually: conditional normal distribution
	public void imputeMissingData(double[] data, boolean[] missing, double[] mean, double[][] cov, double[][] invCov) {
		int dR = 0;
	    for (int i = 0; i < D; i++) {
	      if (missing[i]) {
	        dR++;
	      }
	    }
	    int dO = D - dR;
	    
	    if (dR > 0) {
    	  int random[] = new int[dR];
    	  int observed[] = new int[dO];
    	  int indexOfRandom = 0, indexOfObserved = 0;
    	  double mu1[] = new double[dR];
    	  double mu2[] = new double[dO];
    	  for (int i = 0; i < D; i++) {
    	    if (missing[i]) {
    	      random[indexOfRandom] = i;
    	      mu1[indexOfRandom] = mean[i];
    	      indexOfRandom++;
    	    }
    	    else {
    	      observed[indexOfObserved] = i;
    	      mu2[indexOfObserved] = data[i] - mean[i];
    	      indexOfObserved++;
    	    }
    	  }

    	  double precision[][] = new double[dR][dR];
    	  for (int i = 0; i < dR; i++)
    	    for (int j = 0; j < dR; j++)
    	      precision[i][j] = invCov[random[i]][random[j]];
    	  
    	  double sigma12[][] = new double[dR][dO];
    	  for (int i = 0; i < dR; i++)
    	    for (int j = 0; j < dO; j++)
    	      sigma12[i][j] = cov[random[i]][observed[j]];
    	  
    	  double sigma22[][] = new double[dO][dO];
    	  for (int i = 0; i < dO; i++)
    	    for (int j = 0; j < dO; j++)
    	      sigma22[i][j] = cov[observed[i]][observed[j]];

    	  CholeskyDecomposition sigma22Decomposed = new CholeskyDecomposition(MatrixUtils.createRealMatrix(sigma22));
    	  // sigma12 * (inv(L))^T * inv(L)
    	  RealMatrix invL = MatrixUtils.inverse(sigma22Decomposed.getL());
    	  RealMatrix newSigma12 = MatrixUtils.createRealMatrix(sigma12).multiply(invL.transpose()).multiply(invL);
    	  // sigma12 * mu2 + mu1
    	  double[] newMu1 = newSigma12.operate(mu2);
    	  for (int i = 0; i < dR; i++)
    		  mu1[i] += newMu1[i];

    	  double samples[] = new double[dR];
    	  RealMatrix precisionL = (new CholeskyDecomposition(MatrixUtils.createRealMatrix(precision))).getL();

    	  double z[] = new double[dR];
    	  NormalDistribution nd = new NormalDistribution();
    	  for (int i = 0; i < dR; i++) {
    	    z[i] = nd.sample();
    	  }

    	  for (int i = 0; i < dR; i++) {
    	    samples[i] = z[i];
    	    for (int j = i - 1; j >= 0; j--) {
    	      samples[i] -= precisionL.getEntry(i, j) * z[j];
    	    }
    	    samples[i] /= precisionL.getEntry(i, i);
    	    samples[i] += mu1[i];
    	  }

    	  for (int i = 0; i < dR; i++) 
    	      data[random[i]] = samples[i];
    	}
	    return;
	}
	
	public static int multinomialMembership(double[] data, double[] mixProbs, double[][] means, double[][][] invCovs, double[] determinant) {
		double logProb[] = new double[C];
		for (int i = 0; i < C; i++)
			logProb[i] = Math.log(mixProbs[i]);

		for (int i = 0; i < C; i++) {
			double ay = 0.0;
			double dataSubMean[] = new double[D];
			for (int j = 0; j < D; j++)
				dataSubMean[j] = data[j] - means[i][j];
			
			double ym[] = MatrixUtils.createRealMatrix(invCovs[i]).operate(dataSubMean);
			for (int j = 0; j < D; j++)
				ay += dataSubMean[j] * ym[j];

	 		ay = -0.5 * ay - 0.5 * (Math.log(2 * 3.1415926) * D + Math.log(determinant[i]));
			logProb[i] += ay;
		}

		// find the maximum
		double maxValue = logProb[0];
		for (int i = 1; i < C; i++) {
			if (maxValue < logProb[i]) {
				maxValue = logProb[i];
			}
		}

		// normalizes the weights, and now logProb is the real probability
		for (int i = 0; i < C; i++)
			logProb[i] = Math.exp(logProb[i] - maxValue);

		double sum = 0;
		for(int i = 0; i < C; i++)
			sum += logProb[i];

		for(int i = 0; i < C; i++)
			logProb[i] = logProb[i] / sum;

		// multinomial choice
		int result[] = new int[C];
		StatUtils.multinomial(1, logProb, result);
		for (int i = 0; i < C; i++) {
			if (result[i] > 0) {
				return i;
			}
		}
		return -1;
	}
	
	public static void main(String [] args) {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("My local integration test app");
		SparkContext sc = new SparkContext(conf);
		// testGlobal(sc);
	}
	
}
