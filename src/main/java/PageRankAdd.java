import java.io.IOException;
import java.text.DecimalFormat;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageRankAdd {

    public static class ReadTransition extends Mapper<Object, Text, Text, Text> {
        // input:"b\t0.85"
        // output:<String:"b",double:0.85>
        double beta;
        public void setup(Context context) {
            Configuration config = new Configuration();
            String value = config.get("beta","0.75");
            beta = Double.parseDouble(value);
        }
        public void map(Object key, Text value, Context context) throws  IOException, InterruptedException {
            String[] line = value.toString().trim().split("\\s+");
            String outputKey = line[0];
            Double outputValue = Double.parseDouble(line[1]) * (1-beta);
            context.write(new Text(outputKey), new Text(outputValue.toString()));
        }
    }


    public static class ReadPr extends Mapper<Object, Text, Text, Text>{
        double beta;
        public void setup(Context context) {
            Configuration config = new Configuration();
            String value = config.get("beta","0.75");
            beta = Double.parseDouble(value);
        }
        public void map(Object key, Text value, Context context) throws  IOException, InterruptedException {
            String[] line = value.toString().trim().split("\\s+");
            String outputKey = line[0];
            Double outputValue = Double.parseDouble(line[1]) * beta;
            context.write(new Text(outputKey), new Text(outputValue.toString()));
        }
    }


    public static class Addup extends Reducer<Text, Text, Text, DoubleWritable> {
        // input:<String:"b",{double 0.85, double 0.12, double 0.45...}>
        // output:
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            double sum = 0;
            for (Text elem : values) {
                double value = Double.parseDouble(elem.toString().trim());
                sum = sum + value;
            }
            DecimalFormat df = new DecimalFormat("#.0000");
            sum = Double.valueOf(df.format(sum));
            context.write(key, new DoubleWritable(sum));
        }
    }
    //arg = {beta, UnitMultiply + i, prMatrix + i, prMatrix + (i+1)};
    public static void main(String[] args) throws Exception{

        Configuration config = new Configuration();
        config.set("beta",args[0]);
        Job job = Job.getInstance(config); //
        job.setJobName("AddStep");
        job.setJarByClass(PageRankAdd.class);

        ChainMapper.addMapper(job, ReadTransition.class, Object.class, Text.class, Text.class, Text.class, config);
        ChainMapper.addMapper(job, ReadPr.class, Object.class, Text.class, Text.class, Text.class, config);
        job.setReducerClass(Addup.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, ReadTransition.class);
        MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, ReadPr.class);

        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        job.waitForCompletion(true);
    }
}
