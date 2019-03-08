package vip.anjun.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @author anjun
 * @date 2019-03-07 13:13
 */
public class WordCountMapReducer extends Configured implements Tool {
    // 1 map class Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
    public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text mapOutputKey = new Text();
        private   IntWritable mapOutputValue= new IntWritable(1);
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String lineValue =value.toString();
            StringTokenizer stringTokenizer = new StringTokenizer(lineValue);
            while (stringTokenizer.hasMoreElements()){
                String wordValue = stringTokenizer.nextToken();
                mapOutputKey.set(wordValue);
                context.write(mapOutputKey,mapOutputValue);
            }
        }
    }

    //2 reduce class Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable outputValue = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum =0;
            for(IntWritable value:values){
                sum += value.get();
            }
            outputValue.set(sum);
            context.write(key,outputValue);
        }
    }

    @Override
    public    int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = getConf();
        Job job = Job.getInstance(configuration, this.getClass().getSimpleName());
        job.setJarByClass(this.getClass());

        Path inPath = new Path(args[0]);
        FileInputFormat.addInputPath(job, inPath);

        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
//*********************shuffle start********************
//        job.setPartitionerClass(cls);
//        job.setSortComparatorClass(cls);
//        job.setCombinerClass(cls);
//        job.setGroupingComparatorClass(cls);
// *********************shuffle end********************
        job.setNumReduceTasks(1);

        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        Path outPath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outPath);
        // 设置true 打印log
        boolean isSucess = job.waitForCompletion(true);
        return isSucess ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();

        configuration.set("mapreduce.map.output.compress", "true");
        configuration.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");

        int status = ToolRunner.run(configuration, new WordCountMapReducer(), args);
        System.out.println(status);
    }
}
