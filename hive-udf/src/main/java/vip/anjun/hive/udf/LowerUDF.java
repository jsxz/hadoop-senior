package vip.anjun.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * @author anjun
 * @date 2019-03-08 20:10
 */
public class LowerUDF extends UDF {
    public Text evaluate(Text str){
        if(str.toString() ==null){
            return null;
        }
        return new Text(str.toString().toLowerCase());
    }

    public static void main(String[] args) {
        System.out.println(new LowerUDF().evaluate(new Text("HIVE")));
    }
}
