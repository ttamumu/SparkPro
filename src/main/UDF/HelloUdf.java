import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * Created by tt on 2018/11/5.
 */
public class HelloUdf  {

    /* public Text evaluate(Text input){
         return new Text("hello" + input.toString());
     }*/

    public static void main(String[] args) {
       // HelloUdf udf  = new HelloUdf();
        //Text result = udf.evaluate(new Text("张三"));
        System.out.println("Hello World!!!");
    }
}
