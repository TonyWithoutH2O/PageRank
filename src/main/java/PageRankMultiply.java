import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.util.ArrayList;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;

public class PageRankMultiply {

    public static class TranMatrix extends Mapper<LongWritable, Text, Text, Text> {

        //read transition matrix data
        //Input:a   b,c,d
        //output:<a,b=1/3>
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] line = value.toString().trim().split("\\s+");
            if (line.length < 2 || line[1] == "") {
                return;
            }

            String from = line[0];
            String[] to = line[1].trim().split(",");
            for (String elem : to) {
                context.write(new Text(from), new Text(elem + "=" + String.valueOf((double)1/to.length)));
            }
        }
    }

    public static class PrMatrix extends Mapper<LongWritable, Text, Text, Text> {
        //Read PR-Matrix data
        //Input:a   0.25
        //Output:a  0.25
        public void map (LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] line = value.toString().trim().split("\\s+");
            if (line.length < 2) {
                return;
            }

            context.write(new Text(line[0]), new Text(line[1]));
        }
    }

    public static  class Multiply extends Reducer<Text,Text,Text,Text> {

        public void reduce(Text key, Iterable<Text> value, Context context) throws  IOException, InterruptedException{
            //Iterate the elem in value list . There are two of them 1): "b=0.33", it is the prob that key can reach to b; 2)"0.25",key's weight
            //Input: <a,b=0.33>, <a,c=0.33>... <a,0.25>
            //Output:<b,0.33*0.25>, <c,0.33*0.25>...
            ArrayList<String>  gain = new ArrayList<String>();
            double prValue = 0.0;
            for (Text elem : value) {
                String element = elem.toString().trim();
                if (element.contains("=")) {
                    gain.add(element);
                } else {
                    prValue = Double.parseDouble(element);
                }
            }
            //input:"b=0.33" , "c=0.33", "d=0.33"
            //output:<b,0.33*prValue>
            for (String elem : gain) {
                String outputKey = elem.trim().split("=")[0];
                double relation = Double.parseDouble(elem.trim().split("=")[1]);
                String outputValue = "" + String.valueOf(relation * prValue);
                context.write(new Text(outputKey), new Text(outputValue));
            }
        }
     }

     //args transitionMatrix, PageRankMatrix + i, UnitMultiply + i
    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();

        Job job = Job.getInstance(config); //
        job.setJobName("MultiplyStep");
        job.setJarByClass(PageRankMultiply.class);
        JobConf jc = new JobConf();
        ChainMapper.addMapper(job, TranMatrix.class, LongWritable.class, Text.class, Text.class, Text.class, config);
        ChainMapper.addMapper(job, PrMatrix.class, Text.class, Text.class, Text.class, Text.class, config);

        job.setReducerClass(Multiply.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TranMatrix.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PrMatrix.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);
    }
}

