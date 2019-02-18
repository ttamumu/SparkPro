import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by tt on 2019/2/14.
 */
public class WordCount2 {

    public static class TokenizerMapper
            extends Mapper<Object,Text,Text,IntWritable>{
        public TokenizerMapper(Object key,Text values,Context context) throws IOException,InterruptedException{
            private static IntWritable one = new IntWritable(1);
            private Text text = new Text();
            StringTokenizer itr = new StringTokenizer(values.toString());
            while(itr.hasMoreTokens()){
                text.set(itr.nextToken());
                context.write(text,one);
            }
        }
    }

    public static class IntSumReducer
        extends Reducer<Text,IntWritable,Text,IntWritable>{
        private IntWritable reasult = new IntWritable();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values){
                sum += value.get();
            }
            reasult.set(sum);
            context.write(key,reasult);
        }
    }
}
