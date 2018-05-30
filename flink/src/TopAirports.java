import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.util.List;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.operators.Order;

public class TopAirports {
	public static void main(String[] args) throws Exception {
		ParameterTool parameters = ParameterTool.fromArgs(args);
		String year = parameters.get("year", "0");
		String size = parameters.get("size", "0");
		String inpath = parameters.get("inpath", "0");
		String outpath = parameters.get("outpath", "0");
		// obtain an execution environment
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// Define a data set from the flights file , include the required fields for
		// the task, in this case date and departure
		DataSet<Tuple3<String, String, String>> flights = env
				.readCsvFile(inpath+"ontimeperformance_flights_"+size+".csv")
				.includeFields("0001100001").ignoreFirstLine().ignoreInvalidLines().types(String.class, String.class, String.class);

		DataSet<Tuple1<String>> result = flights.flatMap(new MapDate(year));
		List<Tuple2<String,Integer>> results = result
			.groupBy(0)
			.reduceGroup(new FCounter())
			.sortPartition(1, Order.DESCENDING)
			.setParallelism(1)
			.first(3)
			.collect();
		
		try {
            Writer output = null;
            String path = outpath+"TopAirports_results_"+size+".txt";
            File file = new File(path);
            output = new BufferedWriter(new FileWriter(file));
            for (Tuple2<String, Integer> row : results) {
            	output.write(row.f0+"\t"+row.f1+"\n");
            }
            output.close();
            System.out.println("Wrote to file "+path);
        } catch (Exception e) {
            System.err.println(e);
        }

	}

	public static class MapDate implements FlatMapFunction<Tuple3<String, String, String>,Tuple1<String>> {
		private final String year;
		public MapDate(String year2) {
		    this.year = year2;
		}
		@Override
		public void flatMap(Tuple3<String, String, String> flights, Collector<Tuple1<String>> out) throws Exception {
			if (flights.f0.split("-")[0].equals(year) && !flights.f2.equals("")) {
				out.collect(new Tuple1<String>(flights.f1));
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
