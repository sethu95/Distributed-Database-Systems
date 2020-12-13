import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class equijoin {

	public static class EquiJoinReducer extends Reducer<DoubleWritable, Text, Object, Text> {
		public void reduce(DoubleWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			List<String> table1Rows = new ArrayList<String>();
			List<String> table2Rows = new ArrayList<String>();
			String table1 = "";
			List<String> rowsAll = getRows(values);
			if (rowsAll.size() < 2) {
				return;
			} else {
				table1 = rowsAll.get(0).split(",")[0];
				for (int i=0; i<rowsAll.size(); i++) {
					String tuple = rowsAll.get(i);
					if (table1.equals(tuple.split(",")[0])) {
						table1Rows.add(tuple);
					} else {
						table2Rows.add(tuple);
					}
				}
				if(table1Rows.size() == 0 || table2Rows.size() == 0) {
					return;
				} else {
					Text finalText = getFinalText(table1Rows, table2Rows);
					context.write(null, finalText);
				}
			}
		}

		private Text getFinalText(List<String> table1Rows, List<String> table2Rows) {
			Text finalText = new Text();
			String outputStringRows = "";
			for (int i=0; i<table1Rows.size(); i++) {
				String table1Tuple = table1Rows.get(i);
				for (int j=0; j<table2Rows.size(); j++) {
					String table2Tuple = table2Rows.get(j);
					outputStringRows = outputStringRows + table1Tuple + "," + table2Tuple + "\n";
				}
			}
			finalText.set(outputStringRows.trim());
			return finalText;
		}

		private List<String> getRows(Iterable<Text> values) {
			List<String> rowsAll = new ArrayList<String>();
			for (Text valueEach : values) {
				if(String.valueOf(valueEach).equalsIgnoreCase("null"))
					continue;
				rowsAll.add(valueEach.toString());
			}
			return rowsAll;
		}
	}
	
	public static class EquiJoinMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {
		private Text mapperValue = new Text();
		private DoubleWritable mapperKey = new DoubleWritable();
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			if(String.valueOf(value).equalsIgnoreCase("null"))
				return;
			StringTokenizer rowIterator = new StringTokenizer(value.toString(), "\n");
			while (rowIterator.hasMoreTokens()) {
				String rowEach = rowIterator.nextToken();
				StringTokenizer colIterator = new StringTokenizer(rowEach, ",");
				colIterator.nextToken();
				String id = colIterator.nextToken();
				mapperKey.set(Double.parseDouble(id));
				mapperValue.set(rowEach);
				context.write(mapperKey, mapperValue);
			}
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration c = new Configuration();
		Job job = new Job(c, "equijoin");
		job.setJarByClass(equijoin.class);
		job.setMapperClass(EquiJoinMapper.class);
		job.setReducerClass(EquiJoinReducer.class);
		job.setMapOutputKeyClass(DoubleWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Object.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}