package mljoin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

import mljoin.LDAOutput.LDATuple;

import org.apache.commons.math3.distribution.*;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.rdd.RDD;

import scala.Tuple2;

public class LDAData implements Data {
	public static int WB = 100; 	// total number of wordBlocks
	public static int V = 10000;	// vocabulary size
	public static int WBS = 100;	// wordBlock size = V / WB
	public static int T = 10;		// total number of topics
	
	private static final long serialVersionUID = -8280023097269439963L;
	
	int wordsInDoc[];
	int wordCounts[];
	double docProbs[];
	int docID;
	int wordBlockID;
	
	public LDAData(int docID, int wordBlockID, int[] wordsInDoc, int[] wordCounts) {
		this.docID = docID;
		this.wordBlockID = wordBlockID;
		this.wordsInDoc = wordsInDoc;
		this.wordCounts = wordCounts;
		// initialize docProbs with Dirichlet(prior) <=> Gamma(prior, 1)
		docProbs = new double[T];
		dirichlet(docProbs, 1.0);
	}

	@Override
	public Output process(Model m) {
		// TODO: Jacob
		// update topicsOfWords based on m
		LDAOutput output = multinomialWordTopic(wordsInDoc, wordCounts, docProbs, ((LDAModel)m).getTopicProbs());
		// update docProbs based on topicsOfWords but no need to output it
		int sumByTopic[] = new int[T];
		ArrayList<LDATuple> tuples = output.getTuples();
		for (LDATuple tuple : tuples) {
			sumByTopic[tuple.getTopicID()] += tuple.getCount();
		}
		dirichletConjugate(docProbs, sumByTopic, 1.0);
		// output topicsOfWords
		return output;
	}
	
	public void dirichlet(double[] samples, double prior) {
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
	
	public void dirichletConjugate(double[] samples, int[] counts, double prior) {
		double sum = 0.0;
		for (int i = 0; i < samples.length; i++) {
			samples[i] = (new GammaDistribution(counts[i] + prior, 1)).sample();
			sum += samples[i];
		}
		for (int i = 0; i < samples.length; i++) {
			samples[i] /= sum;
		}
	}
	
	public static HashMap<Integer, double[]> dirichletConjugateSplitIntoWordBlocks(int[] counts, double prior) {
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
	
	public void multinomial(int N, double[] p, int[] n) {
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
		    	n[k] = new BinomialDistribution(N - sum_n, p[k] / (norm - sum_p)).sample();
			} else {
		    	n[k] = 0;
			}
			sum_p += p[k];
			sum_n += n[k];
		}
	}
	
	public LDAOutput multinomialWordTopic(int[] wordVector, int[] countVector, double[] docVector, double[][] topicMatrix) {
		LDAOutput output = new LDAOutput();
		for (int i = 0; i < wordVector.length; i++) {
			double workProbs[] = new double[docVector.length];
			for (int j = 0; j < docVector.length; j++) {
				workProbs[j] = docVector[j] * topicMatrix[j][wordVector[i]];
			}
			int outputCounts[] = new int[docVector.length];
			// multinomial distribution to generate outputCounts based on countVector[i] and workProbs
			multinomial(countVector[i], workProbs, outputCounts);
			for (int j = 0; j < outputCounts.length; j++) {
				if (outputCounts[j] > 0) {
					output.addTuple(j, wordBlockID * WBS + wordVector[i], outputCounts[j]);
				}	
			}
		}
		return output;
	}
	
	public static void main(String [] args) {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("My local integration test app");
		SparkContext sc = new SparkContext(conf);
		testLocal(sc, 6729);
	}
	
	public static RDD<Output> testLocal(SparkContext sc, int listenerPortNumber) {
		ArrayList<Integer> wordBlockIDs = new ArrayList<Integer>();
		for (int i = 0; i < WB; i++)
			wordBlockIDs.add(i);
		JavaSparkContext jsc = new JavaSparkContext(sc);
		JavaRDD<String> initialData = jsc.textFile("wiki_en_bow.mm");
		JavaRDD<Tuple2<Integer, Data>> data = initialData.map(new Function<String, Tuple2<Integer, Data>>() {

			@Override
			public Tuple2<Integer, Data> call(String line)
					throws Exception {
				String[] splits = line.split(" ");
				String[] text = splits[2].split(",");
				int[] wordsInDoc = new int[text.length / 2];
				int[] wordCounts = new int[text.length / 2];
				for (int i = 0; i < text.length / 2; i++) {
					wordsInDoc[i] = Integer.parseInt(text[2 * i]);
					wordCounts[i] = Integer.parseInt(text[2 * i + 1]);				
				}
				return new Tuple2<Integer, Data>(Integer.parseInt(splits[1]), new LDAData(Integer.parseInt(splits[0]), Integer.parseInt(splits[1]), wordsInDoc, wordCounts));
			}
			
		});
		JavaRDD<Tuple2<Integer, Model>> models = jsc.parallelize(wordBlockIDs).map(wordBlockID -> new Tuple2<Integer, Model>(wordBlockID, new LDAModel(wordBlockID)));
		RDD<Output> output = (new MLJoin()).local(sc, models.rdd(), data.rdd(), listenerPortNumber, true);
		// update model.topicProbs based on all data
		output.toJavaRDD().flatMap(
				o -> ((LDAOutput) o).getTuples()).mapToPair(
				tuple -> new Tuple2<Integer, LDATuple>(tuple.getTopicID(), tuple)).aggregateByKey(
				new int[V], (x, y) -> {x[y.getWordID()] += y.getCount(); return x;}, (x, y) -> {for (int i = 0; i < V; i++) x[i] += y[i]; return x;}).flatMapToPair(
						new PairFlatMapFunction<Tuple2<Integer, int[]>, Integer, Tuple2<Integer, double[]>>() {
							
				            @Override  
				            public Iterable<Tuple2<Integer, Tuple2<Integer, double[]>>> call(Tuple2<Integer, int[]> tuple) throws Exception {
				            	int topicID = tuple._1;
				            	HashMap<Integer, double[]> topicProbs = dirichletConjugateSplitIntoWordBlocks(tuple._2, 1.0);
								ArrayList<Tuple2<Integer, Tuple2<Integer, double[]>>> result = new ArrayList<>();
								for (HashMap.Entry<Integer, double[]> entry : topicProbs.entrySet()) {
									Tuple2<Integer, double[]> newtuple = new Tuple2<Integer, double[]>(topicID, entry.getValue());
									result.add(new Tuple2<Integer, Tuple2<Integer, double[]>>(entry.getKey(), newtuple));
								}
								return result;  
				            }  
		        }).join(models.mapToPair(model -> new Tuple2<Integer, Model>(model._1, model._2))).map(
		        x -> {((LDAModel) x._2._2).setTopicRow(x._2._1._1, x._2._1._2); return new Tuple2<Integer, Model>(x._1, x._2._2);});
		return output;
	}
	
}
