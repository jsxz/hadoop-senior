package vip.anjun.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;


/**
 * @author anjun
 * @date 2019-03-07 09:13
 */
public class HdfsApp {
    public static FileSystem getFileSystem() throws IOException {
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(conf);
        return fileSystem;
    }

    public static void read(String fileName) throws IOException {
        FileSystem fileSystem = getFileSystem();

        Path readPath = new Path(fileName);

        FSDataInputStream inputStream = fileSystem.open(readPath);

        try {
            IOUtils.copyBytes(inputStream, System.out, 4096, false);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(inputStream);
        }
    }

    public static void main(String[] args) throws IOException {
        String fileName = "/bd/workspace/hadoop-senior/input/wc.input";
        read(fileName);

        FileSystem fileSystem = getFileSystem();
        String putFileName = "/bd/workspace/hadoop-senior/output/";
        Path writePath = new Path(putFileName);
        FSDataOutputStream outputStream = fileSystem.create(writePath);

        Path readPath = new Path(fileName);
        FSDataInputStream inputStream = fileSystem.open(readPath);

        try {
            IOUtils.copyBytes(inputStream, outputStream, 4096, false);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(inputStream);
            IOUtils.closeStream(outputStream);
        }

    }
}
