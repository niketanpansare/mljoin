package mljoin;

import org.apache.commons.math3.linear.*;

public class GMMModel2 implements Model2 {
	private static final long serialVersionUID = -1953439650104092103L;
	
	int clusterID;
	double mixProb;
	double[] mean;
	double[][] cov;
	double[][] invCov;
	double determinant;
	
	public GMMModel2(int clusterID) {
		this.clusterID = clusterID;
		mixProb = 1.0 / GMMData2.C;
		mean = new double[GMMData2.D];
		cov = new double[GMMData2.D][GMMData2.D];
		for (int i = 0; i < GMMData2.D; i++)
			cov[i][i] = 1.0;		// initialize diagonal matrix
	}
	
	public double getMixProb() {
		return mixProb;
	}

	public void setMixProb(double mixProb) {
		this.mixProb = mixProb;
	}

	public double[] getMean() {
		return mean;
	}

	public void setMean(double[] mean) {
		this.mean = mean;
	}
	
	public double[][] getCov() {
		return cov;
	}
	
	public void setCov(double[][] cov) {
		this.cov = cov;
	}
	
	public double[][] getInvCov() {
		return invCov;
	}
	
	public double getDeterminant() {
		return determinant;
	}

	public Model2 process() {
		// TODO: Jacob
		// inverse cov and store in invCov
		LUDecomposition covDecomposed = new LUDecomposition(MatrixUtils.createRealMatrix(cov));
		invCov = covDecomposed.getSolver().getInverse().getData();
		determinant = covDecomposed.getDeterminant();
		return this;
	}

}
