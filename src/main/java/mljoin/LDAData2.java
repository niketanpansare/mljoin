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
	public static int D = 4010649;	// total number of documents
	
	private static final long serialVersionUID = -8280023097269439963L;
	
	LDADataPart1 data1;
	LDADataPart2 data2;
	
	public LDAData2(LDADataPart1 data1, LDADataPart2 data2) {
		this.data1 = data1;
		this.data2 = data2;
	}
	
	public LDAData2(int docID, int wordBlockID, int[] wordsInDoc, int[] wordCounts) {
		data1 = new LDADataPart1(wordBlockID, wordsInDoc, wordCounts);
		data2 = new LDADataPart2();
	}

	public ArrayList<Delta2> process(Model2 m) {
		// TODO: Jacob
		// update topicsOfWords based on m
		long start  = System.nanoTime();
		ArrayList<Delta2> tuples = multinomialWordTopic(data1.getWordsInDoc(), data1.getWordCounts(), data2.getDocProbs(), ((LDAModel2)m).getTopicProbs());
		// update docProbs based on topicsOfWords but no need to output it
		int sumByTopic[] = new int[T];
		for (Delta2 tuple : tuples) {
			sumByTopic[((LDADelta2)tuple).getTopicID()] += 1;
		}
		double docProbs[] = new double[T];
		StatUtils.dirichletConjugate(docProbs, sumByTopic, 1.0);
		data2.setDocProbs(docProbs);
		// output topicsOfWords
		Statistics.dataProcessTime().addAndGet(System.nanoTime()-start);
		return tuples;
	}
	
	public ArrayList<Delta2> multinomialWordTopic(int[] wordVector, int[] countVector, double[] docVector, double[][] topicMatrix) {
		ArrayList<Delta2> tuples = new ArrayList<Delta2>();
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
				tuples.add(new LDADelta2(StatUtils.categorical(workProbs), data1.getWordBlockID() * WBS + wordVector[i]));
			}
		}
		return tuples;
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
