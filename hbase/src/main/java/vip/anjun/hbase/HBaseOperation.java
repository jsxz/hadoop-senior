package vip.anjun.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InterruptedIOException;

/**
 * CRUD operaion
 *
 * @author anjun
 * @date 2019-03-11 15:31
 */
public class HBaseOperation {

    public static HTable getTableByTableName(String tableName) throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        HTable table = new HTable(configuration, tableName);
        return table;
    }

    private static void printCell(Cell cell) {
        System.out.println(
                Bytes.toString(CellUtil.cloneFamily(cell)) + ":"
                        + Bytes.toString(CellUtil.cloneQualifier(cell))
                        + Bytes.toString(CellUtil.cloneValue(cell))

        );
    }

    public static void getData(HTable table) throws IOException {

        //get rowkey
        Get get = new Get(Bytes.toBytes("1000"));

        get.addColumn(
                Bytes.toBytes("info"),
                Bytes.toBytes("name")
        );
        get.addColumn(
                Bytes.toBytes("info"),
                Bytes.toBytes("age")
        );
        // get data
        Result result = table.get(get);
        for (Cell cell : result.rawCells()) {
            printCell(cell);
        }

    }

    public static void doPut(HTable table) throws InterruptedIOException, RetriesExhaustedWithDetailsException {
        Put put = new Put(Bytes.toBytes("1004"));
        put.add(
                Bytes.toBytes("info"),
                Bytes.toBytes("name"),
                Bytes.toBytes("zhaoliu")

        );
        put.add(
                Bytes.toBytes("info"),
                Bytes.toBytes("age"),
                Bytes.toBytes("23")

        );
        table.put(put);
    }

    public static void main(String[] args) throws IOException {
        String tableName = "user";
        HTable table = null;
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes("1001"));
        scan.setStopRow(Bytes.toBytes("1004"));
//        scan.setFilter(filter);
        ResultScanner resultScanner = table.getScanner(scan);
        try {
            table = getTableByTableName(tableName);
//            getData(table);
//        doPut(table);
//        doDelete(table);
            scan(table, resultScanner);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(table);
            IOUtils.closeStream(resultScanner);
        }
    }

    private static void scan(HTable table, ResultScanner resultScanner) throws IOException {

        for (Result result : resultScanner) {
            System.out.println(Bytes.toString(result.getRow()));
            for (Cell cell : result.rawCells()) {
                printCell(cell);
            }
            System.out.println("---------------");
        }
    }

    private static void doDelete(HTable table) throws IOException {
        Delete delete = new Delete(Bytes.toBytes("1004"));
        delete.deleteFamily(Bytes.toBytes("info"));
        table.delete(delete);
    }
}
