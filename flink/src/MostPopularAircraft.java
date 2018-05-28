import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import org.apache.flink.api.common.operators.Order;


public class MostPopularAircraft {
	
	
	public static void main(String[] args) throws Exception {

		// obtain an execution environment
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// Define a data set from the airlines file , include the required fields for
		// the task, in this case carrier_code, airline name and country
		DataSet<Tuple3<String, String, String>> airlines = env
				.readCsvFile("hdfs://127.0.0.1:9000/user/hadoop/ontimeperformance_airlines.csv")
				.includeFields("111").ignoreFirstLine().ignoreInvalidLines().types(String.class, String.class, String.class);
	
		
		// Define a data set from the FLIGHTS file , include the required fields for
		// the task, in this case airport_code
		DataSet<Tuple2<String, String>> flights = env
				.readCsvFile("hdfs://127.0.0.1:9000/user/hadoop/ontimeperformance_flights_medium.csv").includeFields("010000100000")
				.ignoreFirstLine().ignoreInvalidLines().types(String.class, String.class);
		
		// define a datadet from the AIRCRAFTS file, include required fields for task
		// in this case, 
		DataSet<Tuple3<String, String, String>> aircraft = env
				.readCsvFile("hdfs://127.0.0.1:9000/user/hadoop/ontimeperformance_aircrafts.csv").includeFields("101010000")
				.ignoreFirstLine().ignoreInvalidLines().types(String.class, String.class, String.class);

		DataSet<Tuple3<String, String, String>> USairlines = airlines.filter(new StringFilter());
		DataSet<Tuple2<String, String>> Tailcodes = flights.filter(new tailcodeFilter());
		
		//USairlines.first(10).print(); // show filter worked
		
		// Join to data sets to create tuple data set with tail_numbers, airline name
		DataSet<Tuple2<String,String>> resultJoin1 = USairlines.join(Tailcodes).where(0).equalTo(0)// joining two data sets
																			.with(new JoinAlF()); // using a new join function to create the tuple
		System.out.println("First Join:");
		//resultJoin1.first(10).print(); //show join worked
		
		// need tuple 3 for airline name, manufacturer and model
		DataSet<Tuple3<String, String, String>> resultJoin2 = resultJoin1.join(aircraft).where(1).equalTo(0).with(new JoinAR());
																				
		// Output should be Airline_name, Manufacturer, Model
		System.out.println("!!! Second Join:");
		//resultJoin2.groupBy(0, 1, 2).first(10).print();
		
		DataSet<Tuple3<String,String,Integer>> resultCount = resultJoin2
				.groupBy(0, 1, 2) 
				.reduceGroup(new PCounter())
				.sortPartition(0, Order.DESCENDING)
				.sortPartition(2, Order.DESCENDING);
		System.out.println("Count Result:");
		resultCount.first(50).print();

	}
	
	// need to match on the f2 column of the second join ouptput and count.
	public static class PCounter implements GroupReduceFunction<Tuple3<String,String,String>, Tuple3<String, String, Integer>> {
		@Override
		public void reduce(Iterable<Tuple3<String,String,String>> records, Collector<Tuple3<String, String, Integer>> out) throws Exception {
			String model = null;
			String name = null;
			int cnt = 0;
			for (Tuple3<String,String,String> m : records) {
				name = m.f0;
				model = m.f1+" "+m.f2;
				cnt++;
			}
			out.collect(new Tuple3<>(name, model, cnt));
		}
	}
	
	public static class JoinAR implements FlatJoinFunction<Tuple2<String, String>, Tuple3<String,String,String>, Tuple3<String,String,String>> {
		public void join(Tuple2<String,String> result1, Tuple3<String,String,String> aircrafts, Collector<Tuple3<String,String,String>> out) throws Exception {
			out.collect(new Tuple3<String,String,String>(result1.f0, aircrafts.f1, aircrafts.f2));
		}
	}
	public static class JoinAlF implements FlatJoinFunction<Tuple3<String, String, String>, Tuple2<String,String>, Tuple2<String,String>> {
		public void join(Tuple3<String, String,String> airlines, Tuple2<String,String> flights, Collector<Tuple2<String,String>> out) throws Exception {
			if (!flights.f1.equals("")) {
				out.collect(new Tuple2<String,String>(airlines.f1,flights.f1));
			}
		}
	}

	// FilterFunction that filters out all Strings that == United States.
	public static class StringFilter implements FilterFunction<Tuple3<String, String,String>> {
		@Override
		public boolean filter(Tuple3<String, String,String> data) throws Exception {
			return data.f2.equals("United States");
		}
	}	
	public static class tailcodeFilter implements FilterFunction<Tuple2<String, String>> {
		@Override
		public boolean filter(Tuple2<String, String> data) throws Exception {
			return !data.f1.equals("");
		}
	}

}