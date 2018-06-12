package mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main2 {

	/**
	 * 
	 * The mapper split the input and write in output the key and the real time
	 * whether it solved the problem
	 *
	 */
	static class TestMapper extends Mapper<LongWritable, Text, TimeSolver, Text> {
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, TimeSolver, Text>.Context context)
				throws IOException, InterruptedException {
			String[] row = value.toString().split("\t");
			/** row[0] is SOLVER */
			/** row[11] is REAL */
			/** row[14] is RESULT */
			if (row[14].contains("solved")) {
				context.write(new TimeSolver(row[0], row[11]), new Text(row[11] + "\t" + row[2]));
			}
		}
	}

	/**
	 * 
	 * The reducer take the mapper input and just write it in the file in order to reuse it in
	 * the next job
	 *
	 */
	static class TestReducer extends Reducer<TimeSolver, Text, Text, Text> {
		@Override
		protected void reduce(TimeSolver key, Iterable<Text> values,
				Reducer<TimeSolver, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			StringBuilder str = new StringBuilder();
			for (Text value : values) {
				str.append(value);
				str.append("\t");
			}
			context.write(new Text(key.solver), new Text(str.toString()));
		}
	}

	/**
	 * 
	 * The mapper split the middle input and write in output the pair [key,index] and
	 * the time
	 *
	 */
	static class GroupMapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] row = value.toString().split("\t");
			for (int i = 1; i < row.length; i++) {
				context.write(new Text(), new Text(row[0] + "\t" + row[i]));
			}

		}
	}

	/**
	 * 
	 * For each type store its value in a map and list, sort the list of type and print write
	 * output the time in the same order for each tuple
	 *
	 */
	static class GroupReducer extends Reducer<IntWritable, Text, Text, Text> {
		static boolean start = true;

		@Override
		protected void reduce(IntWritable key, Iterable<Text> values,
				Reducer<IntWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			StringBuilder str = new StringBuilder();
			Map<String, String> times = new HashMap<>();
			List<String> types = new ArrayList<>();
			for (Text value : values) {
				String[] type = value.toString().split("\t");
				if (!times.containsKey(type[0])) {
					times.put(type[0], type[1]);
					types.add(type[0]);
				}
			}
			types.sort(new Comparator<String>() {
				public int compare(String arg0, String arg1) {
					return arg0.compareTo(arg1);
				};
			});
			if (start) {
				start = false;
				str.append("n,\t");
				for (String s : types) {
					str.append(s + ",\t");

				}
				context.write(new Text(""), new Text(str.toString().substring(0, str.toString().length() - 2)));
				str = new StringBuilder("");
			}
			str.append(String.valueOf(key.get()) + ",\t");
			for (String s : types) {
				str.append(times.get(s) + ",\t");
			}
			context.write(new Text(""),
					new Text(str.toString().substring(0, str.toString().length() - 2)));
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job1 = Job.getInstance(conf, "Job1");

		job1.setMapperClass(TestMapper.class);
		job1.setReducerClass(TestReducer.class);
		job1.setGroupingComparatorClass(TimeSolverGroupComparator.class);

		job1.setOutputKeyClass(TimeSolver.class);
		job1.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/init"));
		boolean success = job1.waitForCompletion(true);
		if (success) {
			Job job2 = Job.getInstance(conf, "Job2");

			job2.setMapperClass(GroupMapper.class);
			job2.setReducerClass(GroupReducer.class);

			job2.setOutputKeyClass(IntWritable.class);
			job2.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(job2, new Path(args[1] + "/init"));
			FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/final"));

			System.exit(job2.waitForCompletion(true) ? 0 : 1);

		} else {
			System.exit(1);
		}
	}

	/**--------------------------------------------------------------------------------**/
	/** SORT **/
	/**--------------------------------------------------------------------------------**/

	/**
	 * 
	 * This subclass is a key used to sort it, and it contains just solver name and
	 * its time to solve it whether it solver an instance
	 *
	 */
	public static class TimeSolver implements WritableComparable<TimeSolver> {

		private String solver;
		private String time;

		public TimeSolver() {
		}

		public TimeSolver(String solver, String time) {
			super();
			this.solver = solver;
			this.time = time;
		}

		@Override
		public void readFields(DataInput dataInput) throws IOException {
			solver = WritableUtils.readString(dataInput);
			time = WritableUtils.readString(dataInput);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			WritableUtils.writeString(out, solver);
			WritableUtils.writeString(out, time);
		}

		@Override
		public int compareTo(TimeSolver o) {
			int cmp = solver.compareTo(o.solver);
			double a = Double.parseDouble(time);
			double b = Double.parseDouble(o.time);
			if (cmp == 0) {
				if (a > b)
					return 1;
				else if (a < b)
					return -1;
				else
					return 0;
			}
			return cmp;
		}

		@Override
		public String toString() {
			return String.format("%s %s", solver, time);
		}

		public String getSolver() {
			return solver;
		}

		public void setSolver(String solver) {
			this.solver = solver;
		}

		public String getTime() {
			return time;
		}

		public void setTime(String time) {
			this.time = time;
		}

	}

	/**
	 * 
	 * It used to group in the reducer
	 *
	 */
	public static class TimeSolverGroupComparator extends WritableComparator {

		protected TimeSolverGroupComparator() {
			super(TimeSolver.class, true);
		}

		@Override
		@SuppressWarnings("rawtypes")
		public int compare(WritableComparable a, WritableComparable b) {
			TimeSolver ac = (TimeSolver) a;
			TimeSolver bc = (TimeSolver) b;
			if (ac.solver.equals(bc.solver))
				return 0;
			return 1;
		}

	}

}
