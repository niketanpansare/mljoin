package mljoin;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class GMMData implements Data {
	private static final long serialVersionUID = -8280023097269439963L;
	
	double[] data;
	public GMMData(double[] data) {
		this.data = data;
	}
	
	@Override
	public Output process(Model m) {
		// TODO: Jacob
		return null;
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
				return new Tuple2<Integer, Model>(rand.nextInt(20), new GMMModel(null, null));
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
