import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.spark.sql.catalyst.expressions.In;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by tt on 2019/2/18.
 */
public class WordCount3 {

    public static class TokenizerMapper3
        extends Mapper<Object,Text,Text,IntWritable>{

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            private static IntWritable one = new IntWritable(1);
            private StringTokenizer stringTokenizer = new StringTokenizer(value.toString());
            while (stringTokenizer.hasMoreTokens()){
                Text text = new Text(stringTokenizer.nextToken());
                context.write(text,one);
            }
        }
    }

    public static class IntSumWord
        extends Reducer<Text,IntWritable,Text,IntWritable>{

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            private static IntWritable result = new IntWritable();
            int sum = 0;
            for ( IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key,result);
        }
    }
}
