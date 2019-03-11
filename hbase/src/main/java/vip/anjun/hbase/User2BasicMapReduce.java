package vip.anjun.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * @author anjun
 * @date 2019-03-11 17:56
 */
public class User2BasicMapReduce extends Configured implements Tool {
    private static Text mapOutputKey = new Text();

    public static class ReadUserMapper extends TableMapper<Text, Put> {
        @Override
        public void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            String rowkey = Bytes.toString(key.get());
            mapOutputKey.set(rowkey);
            Put put = new Put(key.get());
            for (Cell cell : value.rawCells()) {
                if ("info".endsWith(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                    put.add(cell);
                }
                if ("age".endsWith(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                    put.add(cell);
                }
            }
            context.write(mapOutputKey, put);
        }
    }

    public static class WriteBasicReducer extends TableReducer<Text, Put, ImmutableBytesWritable> {
        @Override
        protected void reduce(Text key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
            for (Put value : values) {
                context.write(null, value);
            }
        }
    }

    public int run(String[] strings) throws Exception {
//create job
        Job job = Job.getInstance(this.getConf(), this.getClass().getSimpleName());
        job.setJarByClass(this.getClass());

        Scan scan = new Scan();
        scan.setCacheBlocks(false);
        scan.setCaching(500); //每次获取条数
        TableMapReduceUtil.initTableMapperJob(
                "user",
                scan,
                ReadUserMapper.class,
                Text.class,
                Put.class,
                job
        );
        TableMapReduceUtil.initTableReducerJob(
                "basic",
                WriteBasicReducer.class,
                job
        );
        job.setNumReduceTasks(1);
        boolean isSuccess = job.waitForCompletion(true);
        return isSuccess?1:0;
    }

    public static void main(String[] args) throws Exception {
        //get configuration
        Configuration configuration = HBaseConfiguration.create();
        //submit job
        int status = ToolRunner.run(configuration, new User2BasicMapReduce(), args);
        System.out.println(status);
    }
}
