package mljoin;

import java.util.ArrayList;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SQLContext;

public class LDAData2 implements Data2 {
	public static int WB = 100; 	// total number of wordBlocks
	public static int V = 1000000;	// vocabulary size
	public static int WBS = 10000;	// wordBlock size = V / WB
	public static int T = 1250;		// total number of topics
	
	private static final long serialVersionUID = -8280023097269439963L;
	
	int wordsInDoc[];
	int wordCounts[];
	double docProbs[];
	int docID;
	int wordBlockID;
	
	public LDAData2(int docID, int wordBlockID, int[] wordsInDoc, int[] wordCounts) {
		this.docID = docID;
		this.wordBlockID = wordBlockID;
		this.wordsInDoc = wordsInDoc;
		this.wordCounts = wordCounts;
		// initialize docProbs with Dirichlet(prior) <=> Gamma(prior, 1)
		docProbs = new double[T];
		StatUtils.dirichlet(docProbs, 1.0);
	}

	public ArrayList<Delta2> process(Model2 m) {
		// TODO: Jacob
		// update topicsOfWords based on m
		long start  = System.nanoTime();
		LDADelta2a tuples = multinomialWordTopicNew(wordsInDoc, wordCounts, docProbs, ((LDAModel2)m).getTopicProbs());
		// update docProbs based on topicsOfWords but no need to output it
		int sumByTopic[] = new int[T];
		for(int i = 0; i < tuples.topicIDs.length; i++) {
			sumByTopic[tuples.topicIDs[i]] += 1;
		}
		StatUtils.dirichletConjugate(docProbs, sumByTopic, 1.0);
		// output topicsOfWords
		Statistics.dataProcessTime().addAndGet(System.nanoTime()-start);
		ArrayList<Delta2> ret = new ArrayList<Delta2>();
		ret.add(tuples);
		return ret;
	}
	
	public LDADelta2a multinomialWordTopicNew(int[] wordVector, int[] countVector, double[] docVector, double[][] topicMatrix) {
		LDADelta2a ret = new LDADelta2a();
		int numTopics = 0;
		for (int i = 0; i < wordVector.length; i++) {
			numTopics += countVector[i];
		}
		
		ret.topicIDs = new int[numTopics];
		ret.wordIDs = new int[numTopics];
		int iter = 0;
		for (int i = 0; i < wordVector.length; i++) {
			double workProbs[] = new double[T];
			for (int j = 0; j < T; j++) {
				workProbs[j] = docVector[j] * topicMatrix[j][wordVector[i]];
			}
			// int outputCounts[] = new int[T];
			// // multinomial distribution to generate outputCounts based on countVector[i] and workProbs
			// StatUtils.multinomial(countVector[i], workProbs, outputCounts);
			// for (int j = 0; j < T; j++) {
			// 	if (outputCounts[j] > 0) {
			// 		tuples.add(new LDADelta2(j, wordBlockID * WBS + wordVector[i], outputCounts[j]));
			// 	}	
			// }
			for (int k = 0; k < countVector[i]; k++) {
				ret.topicIDs[iter] = StatUtils.categorical(workProbs);
				ret.wordIDs[iter] = wordBlockID * WBS + wordVector[i];
				iter++;
			}
		}
		return ret;
	}
	
	public static void main(String [] args) {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("My local integration test app");
		SparkContext sc = new SparkContext(conf);
		SQLContext sqlCtx = new SQLContext(sc);
		LDADemo demo = new LDADemo();
		
		demo.naiveLDA(sqlCtx, demo.randData(sc), demo.randData(sc), demo.randData(sc));
		// testGlobal(sc);
	}
	
}
