package mljoin;

import java.util.HashMap;

import org.apache.commons.math3.distribution.BinomialDistribution;
import org.apache.commons.math3.distribution.ChiSquaredDistribution;
import org.apache.commons.math3.distribution.GammaDistribution;
import org.apache.commons.math3.distribution.MultivariateNormalDistribution;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.linear.*;

public class StatUtils {
	
	public StatUtils () {}
	
	public static void dirichlet(double[] samples, double prior) {
		GammaDistribution gd = new GammaDistribution(prior, 1);
		double sum = 0.0;
		for (int i = 0; i < samples.length; i++) {
			samples[i] = gd.sample();
			sum += samples[i];
		}
		for (int i = 0; i < samples.length; i++) {
			samples[i] /= sum;
		}
	}
	
	public static void dirichletConjugate(double[] samples, int[] counts, double prior) {
		double sum = 0.0;
		for (int i = 0; i < samples.length; i++) {
			samples[i] = (new GammaDistribution(counts[i] + prior, 1)).sample();
			sum += samples[i];
		}
		for (int i = 0; i < samples.length; i++) {
			samples[i] /= sum;
		}
	}
	
	public static HashMap<Integer, double[]> dirichletConjugateSplitIntoWordBlocks(int[] counts, double prior, int WB, int WBS) {
		HashMap<Integer, double[]> result = new HashMap<>();
		double samples[] = new double[counts.length];
		double sum = 0.0;
		for (int i = 0; i < samples.length; i++) {
			samples[i] = (new GammaDistribution(counts[i] + prior, 1)).sample();
			sum += samples[i];
		}
		for (int i = 0; i < samples.length; i++) {
			samples[i] /= sum;
		}
		for (int i = 0; i < WB; i++) {
			double temp[] = new double[WBS];
			for (int j = 0; j < WBS; j++) {
				temp[j] = samples[i * WBS + j];
			}
			result.put(i, temp);
		}
		return result;
	}
	
	public static void multinomial(int N, double[] p, int[] n) {
		double norm = 0.0;
		double sum_p = 0.0;
		int sum_n = 0;

		/* p[k] may contain non-negative weights that do not sum to 1.0.
		 * Even a probability distribution will not exactly sum to 1.0
		 * due to rounding errors. 
		 */
		for (int k = 0; k < p.length; k++)
			norm += p[k];

		for (int k = 0; k < p.length; k++) {
			if (p[k] > 0.0) {
				if (p[k] / (norm - sum_p) > 1)
					n[k] = new BinomialDistribution(N - sum_n, 1.0).sample();
				else
					n[k] = new BinomialDistribution(N - sum_n, p[k] / (norm - sum_p)).sample();
			} else {
		    	n[k] = 0;
			}
			sum_p += p[k];
			sum_n += n[k];
		}
	}
	
	public static void multivariateNormal(double[] samples, double[] mean, double[][] covariance) {
		MultivariateNormalDistribution mnd = new MultivariateNormalDistribution(mean, covariance);
		samples = mnd.sample();
	}
	
	public static void inverseWishart(double[][] samples, double[][] variance, int iwM) {
		boolean isLegalSample = false;

		// loop till we get a legal sample 
		while (!isLegalSample) {
			double work[][] = new double[variance.length][variance.length];
			for (int i = 0; i < variance.length; ++i) {
				work[i][i] = Math.sqrt((new ChiSquaredDistribution(iwM - i)).sample());
				for (int j = 0; j < i; ++j)
					work[i][j] = (new NormalDistribution()).sample();
			}

			// reverse the scale_matrix
			RealMatrix scaleMatInv = MatrixUtils.inverse(MatrixUtils.createRealMatrix(variance));
			CholeskyDecomposition scaleMatInvDecomposed = new CholeskyDecomposition(scaleMatInv);
			RealMatrix product = scaleMatInvDecomposed.getL().multiply(MatrixUtils.createRealMatrix(work));
			
			// reverse the result 
			samples = MatrixUtils.inverse(product.multiply(product.transpose())).getData();

			boolean isSingular = false;
			for (int i = 0; i < samples.length; i++) {
				double u = samples[i][i];
				if (u == 0) { 												// singular matrix
					isSingular = true;
					break;
				}
			}
			for (int i = 0; i < samples.length; i++) {
				for(int j = 0; j < samples.length; j++) {
					double u = samples[i][j];
					if (Double.isNaN(u) || Double.isInfinite(u)) { 			// singular matrix
						isSingular = true;
						break;
					}
				}
			}
			if (!isSingular)
				isLegalSample = true;
		}
	}
	
}
