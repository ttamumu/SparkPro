import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by tt on 2019/2/12.
 */
public class WorlCount {
    public static class TokenizerMapper
        extends Mapper<Object,Text,Text,IntWritable> {
        public TokenizerMapper(Object key,Text value,Context context) throws IOException,InterruptedException{
            private final static IntWritable one = new IntWritable(1);
            private Text word = new Text();
            StringTokenizer itr = new StringTokenizer(value.toString());
            while(itr.hasMoreTokens()){
                word.set(itr.nextToken());
                context.write(word,one);
            }
        }
    }

    public  static class IntSumReducer
        extends Reducer<Text,IntWritable,Text,IntWritable>{
        private IntWritable result = new IntWritable();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values){
                sum += val.get();
            }
            result.set(sum);
            context.write(key,result);
        }
    }


}
