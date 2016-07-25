package mljoin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import mljoin.LDAOutput.LDATuple;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SQLContext;

import scala.Tuple2;

public class LDAData implements Data {
	public static int WB = 10; 	// total number of wordBlocks
	public static int V = 1000;	// vocabulary size
	public static int WBS = 100;	// wordBlock size = V / WB
	public static int T = 2;		// total number of topics
	
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
		StatUtils.dirichlet(docProbs, 1.0);
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
		StatUtils.dirichletConjugate(docProbs, sumByTopic, 1.0);
		// output topicsOfWords
		return output;
	}
	
	public LDAOutput multinomialWordTopic(int[] wordVector, int[] countVector, double[] docVector, double[][] topicMatrix) {
		LDAOutput output = new LDAOutput();
		for (int i = 0; i < wordVector.length; i++) {
			double workProbs[] = new double[T];
			for (int j = 0; j < T; j++) {
				workProbs[j] = docVector[j] * topicMatrix[j][wordVector[i]];
			}
			int outputCounts[] = new int[T];
			// multinomial distribution to generate outputCounts based on countVector[i] and workProbs
			StatUtils.multinomial(countVector[i], workProbs, outputCounts);
			for (int j = 0; j < T; j++) {
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
		SQLContext sqlCtx = new SQLContext(sc);
		LDADemo demo = new LDADemo();
		
		demo.naiveLDA(sqlCtx, demo.randData(sc), demo.randData(sc), demo.randData(sc));
		// testGlobal(sc);
	}
	
	/***
	public static RDD<Output> testGlobal(SparkContext sc) {
		ArrayList<Integer> wordBlockIDs = new ArrayList<Integer>();
		for (int i = 0; i < WB; i++)
			wordBlockIDs.add(i);
		JavaSparkContext jsc = new JavaSparkContext(sc);
		JavaRDD<String> initialData = jsc.textFile("/Users/jacobgao/Downloads/wordblock.tbl");
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
		RDD<Output> output = (new MLJoin()).global(sc, models.rdd(), data.rdd());
		// update model.topicProbs based on all data
		List<LDAModel> newModels = output.toJavaRDD().flatMap(
				o -> ((LDAOutput) o).getTuples()).mapToPair(
				tuple -> new Tuple2<Integer, LDATuple>(tuple.getTopicID(), tuple)).aggregateByKey(
				new int[V], (x, y) -> {x[y.getWordID()] += y.getCount(); return x;}, (x, y) -> {for (int i = 0; i < V; i++) x[i] += y[i]; return x;}).flatMapToPair(
						new PairFlatMapFunction<Tuple2<Integer, int[]>, Integer, Tuple2<Integer, double[]>>() {
							
				            @Override  
				            public Iterable<Tuple2<Integer, Tuple2<Integer, double[]>>> call(Tuple2<Integer, int[]> tuple) throws Exception {
				            	int topicID = tuple._1;
				            	HashMap<Integer, double[]> topicProbs = StatUtils.dirichletConjugateSplitIntoWordBlocks(tuple._2, 1.0, WB, WBS);
								ArrayList<Tuple2<Integer, Tuple2<Integer, double[]>>> result = new ArrayList<>();
								for (Entry<Integer, double[]> entry : topicProbs.entrySet()) {
									Tuple2<Integer, double[]> newtuple = new Tuple2<Integer, double[]>(topicID, entry.getValue());
									result.add(new Tuple2<Integer, Tuple2<Integer, double[]>>(entry.getKey(), newtuple));
								}
								return result;  
				            }  
		        }).join(models.mapToPair(model -> new Tuple2<Integer, Model>(model._1, model._2))).map(
		        x -> ((LDAModel) x._2._2).setTopicRow(x._2._1._1, x._2._1._2)).distinct().collect();
		return output;
	}
	*/
	
}
