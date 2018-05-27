import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

import org.apache.flink.api.common.operators.Order;


public class MostPopularAircraft {
	
	
	public static void main(String[] args) throws Exception {

		// obtain an execution environment
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// Define a data set from the airlines file , include the required fields for
		// the task, in this case carrier_code, airline name and country
		DataSet<Tuple3<String, String, String>> airlines = env
				.readCsvFile("/Users/william/DATA3404/A1/src/assignment_data_files/ontimeperformance_airlines.csv")
//				.readCsvFile("hdfs://127.0.0.1:9000/home/hadoop/assignment_data_files/ontimeperformance_airlines.csv")
				.includeFields("111").ignoreFirstLine().ignoreInvalidLines().types(String.class, String.class, String.class);
	
		
		// Define a data set from the FLIGHTS file , include the required fields for
		// the task, in this case airport_code
		DataSet<Tuple2<String, String>> flights = env
				.readCsvFile("/Users/william/DATA3404/A1/src/assignment_data_files/ontimeperformance_flights_tiny.csv").includeFields("010000100000")
				.ignoreFirstLine().ignoreInvalidLines().types(String.class, String.class);
		
		// define a datadet from the AIRCRAFTS file, include required fields for task
		// in this case, 
		DataSet<Tuple3<String, String, String>> aircraft = env
				.readCsvFile("/Users/william/DATA3404/A1/src/assignment_data_files/ontimeperformance_aircrafts.csv").includeFields("101010000")
				.ignoreFirstLine().ignoreInvalidLines().types(String.class, String.class, String.class);
		
		
	
		
		DataSet<Tuple3<String, String, String>> carrier_codes_Airlines = airlines.filter(new StringFilter());
		
		carrier_codes_Airlines.first(10).print(); // show filter worked
		
		// Join to data sets to create tuple data set with tail_numbers, airline name
		DataSet<Tuple2<String,String>> resultJoin1 = carrier_codes_Airlines.join(flights).where(0).equalTo(0)// joining two data sets
																			.with(new JoinAlF()); // using a new join function to create the tuple
		System.out.println("First Join:");
		resultJoin1.first(10).print(); //show join worked
		
		// need tuple 3 for airline name, manufacturer and model
		DataSet<Tuple3<String, String, String>> resultJoin2 = resultJoin1.join(aircraft).where(1).equalTo(0).with(new JoinAR());
																				
		// Output should be Airline_name, Manufacturer, Model
		System.out.println("!!! Second Join:");
		resultJoin2.first(10).print();
		

	}
	
	public static class JoinAR implements FlatJoinFunction<Tuple2<String, String>, Tuple3<String,String,String>, Tuple3<String,String,String>> {
		public void join(Tuple2<String,String> result1, Tuple3<String,String,String> airlines, Collector<Tuple3<String,String,String>> out) throws Exception {
			// if the tail numbers match
			if (airlines.f0.equals(result1.f1)) {
				out.collect(new Tuple3<String,String,String>(result1.f0, airlines.f1, airlines.f2));
			}
		}
	}
	public static class JoinAlF implements FlatJoinFunction<Tuple3<String, String, String>, Tuple2<String,String>, Tuple2<String,String>> {
		public void join(Tuple3<String, String,String> airlines, Tuple2<String,String> flights, Collector<Tuple2<String,String>> out) throws Exception {
			// if the filtered carrier code matches the flight carrier code
			if (airlines.f0.equals(flights.f0)) {
				out.collect(new Tuple2<String,String>(airlines.f1,flights.f1));
			}
		}
	}

	// FilterFunction that filters out all Strings that == United States.
	public static class StringFilter implements FilterFunction<Tuple3<String, String,String>> {
	@Override
	public boolean filter(Tuple3<String, String,String> data) throws Exception {
		if (data.f2.equals("United States")) return true;
		return false;
	}
	}

}