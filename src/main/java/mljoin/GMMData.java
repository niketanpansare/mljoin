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

public class GMMData implements Data {
	public static int D = 2; 		// total number of dimensions
	public static int C = 3;		// total number of clusters
	
	private static final long serialVersionUID = -8280023097269439963L;
	
	double[] data;
	boolean[] missing;
	
	public GMMData(double[] data) {
		this.data = data;
		missing = new boolean[D];
		for (int i = 0; i < D; i++) {
			if (data[i] == 0.0)
				missing[i] = true;
		}
	}

	@Override
	public Output process(Model m) {
		// TODO: Jacob
		double[] mean = ((GMMModel)m).getMean();
		double[][] cov = ((GMMModel)m).getCov();
		double[][] invCov = ((GMMModel)m).getInvCov();
		// impute data based on parameters above under initial membership
		imputeMissingData(data, missing, mean, cov, invCov);
		return new GMMOutput(data);
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
		testGlobal(sc);
	}
	
	public static RDD<Output> testGlobal(SparkContext sc) {
		ArrayList<Integer> clusterIDs = new ArrayList<Integer>();
		for (int i = 0; i < C; i++)
			clusterIDs.add(i);
		JavaSparkContext jsc = new JavaSparkContext(sc);
		JavaRDD<String> initialData = jsc.textFile("/Users/jacobgao/Downloads/imputation.tbl");
		JavaRDD<Tuple2<Integer, Data>> data = initialData.map(new Function<String, Tuple2<Integer, Data>>() {

			Random rand = new Random();
			@Override
			public Tuple2<Integer, Data> call(String line)
					throws Exception {
				String[] splits = line.split(" ");
				String[] text = splits[1].split(",");
				double[] point = new double[text.length];
				for (int i = 0; i < text.length; i++)
					point[i] = Double.parseDouble(text[i]);
				int membership = rand.nextInt(C);	// randomly assign to a cluster
				return new Tuple2<Integer, Data>(membership, new GMMData(point));
			}
			
		});
		JavaRDD<Tuple2<Integer, Model>> models = jsc.parallelize(clusterIDs).map(clusterID -> new Tuple2<Integer, Model>(clusterID, new GMMModel(clusterID)));
		RDD<Output> output = (new MLJoin()).global(sc, models.rdd(), data.rdd());
		
		// 1. update data.membership based on all model
		Broadcast<List<Tuple2<Integer, Model>>> evalModels = jsc.broadcast(models.map(m -> new Tuple2<Integer, Model>(m._1, m._2.process())).collect());
		double mixProbs[] = new double[C];
		double means[][] = new double[C][D];
		double invCovs[][][] = new double[C][D][D];
		double determinant[] = new double[C];
		for (Tuple2<Integer, Model> model : evalModels.value()) {
			int clusterID = model._1;
			mixProbs[clusterID] = ((GMMModel)model._2).getMixProb();
			means[clusterID] = ((GMMModel)model._2).getMean();
			invCovs[clusterID] = ((GMMModel)model._2).getInvCov();
			determinant[clusterID] = ((GMMModel)model._2).getDeterminant();
		}
		JavaRDD<Output> updatedOutput = output.toJavaRDD().map(o -> ((GMMOutput) o).setClusterID(multinomialMembership(((GMMOutput) o).getData(), mixProbs, means, invCovs, determinant)));
		
		// calculate prior mean and variance
		long count = updatedOutput.count();
		double priorMean[] = updatedOutput.mapToPair(o -> new Tuple2<Integer, double[]>(0, ((GMMOutput) o).getData())).reduceByKey(
				(x, y) -> {for (int i = 0; i < D; i++) x[i] += y[i]; return x;}).collect().get(0)._2;
		for (int i = 0; i < D; i++)
			priorMean[i] /= count;
		double priorCov[] = updatedOutput.mapToPair(
				o -> {double sumsqr[] = new double[D]; 
					  for (int i = 0; i < D; i++)
							  sumsqr[i] = Math.pow((((GMMOutput) o).getData()[i] - priorMean[i]), 2) / 4.0;
					  return new Tuple2<Integer, double[]>(0, sumsqr);}).reduceByKey(
				(x, y) -> {for (int i = 0; i < D; i++) x[i] += y[i]; return x;}).collect().get(0)._2;
		double priorInvCov[] = new double[D];
		for (int i = 0; i < D; i++) {
			priorCov[i] /= count;
			priorInvCov[i] = 1.0 / priorCov[i];
		}
		
		// 2. update model.mixProb, model.mean, model.cov based on all data
		// 2.1. model.mixProb
		List<Tuple2<Integer, Integer>> clusterSumList = updatedOutput.mapToPair(
				o -> new Tuple2<Integer, Integer>(((GMMOutput) o).getClusterID(), 1)).reduceByKey(
				(x, y) -> x + y).collect();
		int clusterSum[] = new int[C];
		for (Tuple2<Integer, Integer> cluster : clusterSumList)
			clusterSum[cluster._1] = cluster._2;
		StatUtils.dirichletConjugate(mixProbs, clusterSum, 1.0);
		
		// 2.2. model.cov
		List<Tuple2<Integer, double[][]>> covSumList = updatedOutput.mapToPair(
				o -> {double sumsqr[][] = new double[D][D]; 
					  for (int i = 0; i < D; i++) 
						  for (int j = 0; j < D; j++) 
							  sumsqr[i][j] = (((GMMOutput) o).getData()[i] - means[((GMMOutput) o).getClusterID()][i]) 
							  				 * (((GMMOutput) o).getData()[j] - means[((GMMOutput) o).getClusterID()][j]); 
					  return new Tuple2<Integer, double[][]>(((GMMOutput) o).getClusterID(), sumsqr);}).reduceByKey(
				(x, y) -> {for (int i = 0; i < D; i++) for (int j = 0; j < D; j++) x[i][j] += y[i][j]; return x;}).collect();
		double covSum[][][] = new double[C][D][D];
		for (Tuple2<Integer, double[][]> cov : covSumList)
			covSum[cov._1] = cov._2;
		double covs[][][] = new double[C][D][D];
		for (int k = 0; k < C; k++) {
			double variance[][] = new double[D][D];
			for (int i = 0; i < D; i++) {
				for (int j = 0; j < D; j++) {
					variance[i][j] = covSum[k][i][j];
					if (i == j)
						variance[i][j] += priorCov[i];
				}
			}
			int iwM = clusterSum[k] + D + 1;
			StatUtils.inverseWishart(covs[k], variance, iwM);
		}
		
		// 2.3. model.mean
		List<Tuple2<Integer, double[]>> meanSumList = updatedOutput.mapToPair(
				o -> new Tuple2<Integer, double[]>(((GMMOutput) o).getClusterID(), ((GMMOutput) o).getData())).reduceByKey(
				(x, y) -> {for (int i = 0; i < D; i++) x[i] += y[i]; return x;}).collect();
		double meanSum[][] = new double[C][D];
		for (Tuple2<Integer, double[]> mean : meanSumList)
			meanSum[mean._1] = mean._2;
		for (int k = 0; k < C; k++) {
			double mean[] = new double[D];
			double covariance[][] = new double[D][D];
			for (int i = 0; i < D; i++) {
				for (int j = 0; j < D; j++) {
					covariance[i][j] = clusterSum[k] * invCovs[k][i][j];
					if (i == j)
						covariance[i][j] += priorInvCov[i];
				}
			}
			RealMatrix covMat = MatrixUtils.inverse(MatrixUtils.createRealMatrix(covariance));
			for (int i = 0; i < D; i++) {
				mean[i] = MatrixUtils.createRealDiagonalMatrix(priorInvCov).operate(priorMean)[i] 
						  + MatrixUtils.createRealMatrix(invCovs[k]).operate(meanSum[k])[i];
			}
			mean = covMat.operate(mean);
			covariance = covMat.getData();
			StatUtils.multivariateNormal(means[k], mean, covariance);
		}
		
		// final model parameters are mixProbs, means, covs
		return updatedOutput.rdd();
	}
	
	public static RDD<Output> testLocal(SparkContext sc, int listenerPortNumber) {
		ArrayList<Integer> seq = new ArrayList<Integer>();
		for(int i = 0; i < 10000; i++) {
			seq.add(i);
		}
		JavaSparkContext jsc = new JavaSparkContext(sc);
		JavaRDD<Integer> seqRDD = jsc.parallelize(seq);
		
		// This rdd represents the setup model.
		// The first field is key (which should match the key on which the data should be joined)
		// In this example: I assign the keys to the model and data randomly using  rand.nextInt(20)
		JavaRDD<Tuple2<Integer, Model>> models = seqRDD.map(new Function<Integer, Tuple2<Integer, Model>>() {

			Random rand = new Random();
			@Override
			public Tuple2<Integer, Model> call(Integer arg0)
					throws Exception {
				return new Tuple2<Integer, Model>(rand.nextInt(20), new GMMModel(0));
			}
			
		});
		JavaRDD<Tuple2<Integer, Data>> data = seqRDD.map(new Function<Integer, Tuple2<Integer, Data>>() {

			Random rand = new Random();
			@Override
			public Tuple2<Integer, Data> call(Integer arg0)
					throws Exception {
				return new Tuple2<Integer, Data>(rand.nextInt(20), new GMMData(null));
			}
			
		});
		
		return (new MLJoin()).local(sc, models.rdd(), data.rdd(), listenerPortNumber, true);
	}

}
