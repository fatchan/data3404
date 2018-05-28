import java.io.File;
import java.io.BufferedWriter;
import java.io.Writer;
import java.io.FileWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

import org.apache.flink.api.common.operators.Order;

public class AverageDelayUS {
	public static void main(String[] args) throws Exception {
		ParameterTool parameters = ParameterTool.fromArgs(args);
		String year = parameters.get("year", "0");
		// obtain an execution environment
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple3<String, String, String>> airlines = env.readCsvFile("hdfs://127.0.0.1:9000/user/hadoop/ontimeperformance_airlines.csv")
				.includeFields("111").ignoreFirstLine().ignoreInvalidLines().types(String.class, String.class, String.class);
		DataSet<Tuple3<String, String, String>> airlinesFiltered = airlines.filter(new USCodes());
				DataSet<Tuple6<String, String, String, String, String, String>> flights = env
				.readCsvFile("hdfs://127.0.0.1:9000/user/hadoop/ontimeperformance_flights_medium.csv")
				.includeFields("01010001111").ignoreFirstLine().ignoreInvalidLines().types(String.class, String.class, String.class, String.class, String.class, String.class);
		DataSet<Tuple6<String, String, String, String, String, String>> flightsFiltered = flights.filter(new dateFilter(year));
		DataSet<Tuple2<String,Double>> joined = flightsFiltered.join(airlinesFiltered).where(0).equalTo(0).with(new joinAD());
		
		List<Tuple2<String,Integer>> result = joined
			.groupBy(0)
			.reduceGroup(new DelayGrouper())
			.sortPartition(1, Order.DESCENDING)
			.collect();

		try {
            Writer output = null;
            String path = "/home/hadoop/AverageDelayUS_results.txt";
            File file = new File(path);
            output = new BufferedWriter(new FileWriter(file));
            for (Tuple2<String, Integer> row : result) {
            	output.write(row.f0+"\t"+row.f1+"\n");
            }
            output.close();
            System.out.println("Wrote to file "+path);
        } catch (Exception e) {
            System.err.println(e);
        }
		
	}

	public static class USCodes implements FilterFunction<Tuple3<String, String, String>> {
		@Override
		public boolean filter(Tuple3<String, String, String> airlines) {
			return airlines.f2.equals("United States");
		}
	}
	
	public static class dateFilter implements FilterFunction<Tuple6<String, String, String, String, String, String>> {
		private final String year;
		public dateFilter(String year2) {
		    this.year = year2;
		}
		@Override
		public boolean filter(Tuple6<String, String, String, String, String, String> flights) {
			return flights.f1.split("-")[0].equals(year) && !flights.f4.equals("") && !flights.f5.equals("");
		}
	}
	
	public static class joinAD implements JoinFunction<Tuple6<String, String, String, String, String, String>, Tuple3<String, String, String>, Tuple2<String, Double>> {
		@Override
		public Tuple2<String, Double> join(Tuple6<String, String, String, String, String, String> flights, Tuple3<String, String, String> airlines) throws ParseException {
			// get delay of this flight and put airline, delay. then when we reduce, we will get the average.
			SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss"); 
			
			Date departurescheduled = format.parse(flights.f2);
			Date departureactual = format.parse(flights.f4);
			double departuredifference = departureactual.getTime() - departurescheduled.getTime(); 
			
			Date arrivalscheduled = format.parse(flights.f3);
			Date arrivalactual = format.parse(flights.f5);
			double arrivaldifference = arrivalactual.getTime() - arrivalscheduled.getTime(); 
			
			return new Tuple2<String, Double>(airlines.f1, departuredifference+arrivaldifference);
		}
	}

	public static class DelayGrouper implements GroupReduceFunction<Tuple2<String, Double>, Tuple2<String, Integer>> {
		@Override
		public void reduce(Iterable<Tuple2<String, Double>> records, Collector<Tuple2<String, Integer>> out) throws Exception {
			String airline = null;
			int cnt = 0;
			int totaldelay = 0;
			for (Tuple2<String, Double> m : records) {
				airline = m.f0;
				totaldelay += m.f1;
				cnt++;
			}
			int average = totaldelay/cnt;
			out.collect(new Tuple2<>(airline, average));
		}
	}
}
