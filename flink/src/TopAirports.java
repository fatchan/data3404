import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.operators.Order;

public class TopAirports {
	public static void main(String[] args) throws Exception {
		ParameterTool parameters = ParameterTool.fromArgs(args);
		String year = parameters.get("year", "0");
		// obtain an execution environment
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// Define a data set from the flights file , include the required fields for
		// the task, in this case date and departure
		DataSet<Tuple2<String, String>> flights = env
				//.readCsvFile("hdfs://127.0.0.1:9000/user/hadoop/ontimeperformance_flights_tiny.csv")
				//.readCsvFile("hdfs://127.0.0.1:9000/user/hadoop/ontimeperformance_flights_small.csv")
				.readCsvFile("hdfs://127.0.0.1:9000/user/hadoop/ontimeperformance_flights_medium.csv")
				.includeFields("00011").ignoreFirstLine().ignoreInvalidLines().types(String.class, String.class);
		// Define a data set from the airports file , include the required fields for
		// the task, in this case airport_code
		DataSet<Tuple1<String>> airports = env
				.readCsvFile("hdfs://127.0.0.1:9000/user/hadoop/ontimeperformance_airports.csv").includeFields("1")
				.ignoreFirstLine().ignoreInvalidLines().types(String.class);

		DataSet<Tuple1<String>> result = flights.join(airports).where(1).equalTo(0)// joining two data sets
			.with(new JoinAF(year)); // using a new join function to create the tuple
		result
			.groupBy(0)// group according to region name
			.reduceGroup(new FCounter()).sortPartition(1, Order.DESCENDING).first(3).print();

	}

	public static class JoinAF implements FlatJoinFunction<Tuple2<String, String>, Tuple1<String>, Tuple1<String>> {
		private final String year;
		public JoinAF(String year2) {
		    this.year = year2;
		}
		public void join(Tuple2<String, String> flights, Tuple1<String> airports, Collector<Tuple1<String>> out) throws Exception {
			if (flights.f0.split("-")[0].equals(year)) {
				out.collect(new Tuple1<String>(airports.f0));
			}
		}
	}

	public static class FCounter implements GroupReduceFunction<Tuple1<String>, Tuple2<String, Integer>> {
		@Override
		public void reduce(Iterable<Tuple1<String>> records, Collector<Tuple2<String, Integer>> out) throws Exception {
			String airport = null;
			int cnt = 0;
			for (Tuple1<String> m : records) {
				airport = m.f0;
				cnt++;
			}
			out.collect(new Tuple2<>(airport, cnt));
		}

	}

}
