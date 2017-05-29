import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
public class TFIDF_step1 { 
    public static class Map
    extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable un = new IntWritable(1);
        private static       Text   word_doc = new Text();
        private static Set<String> slist;
        static {
            slist = new HashSet<String>();
            slist.add("and");
            slist.add("are");   
            slist.add("com");
            slist.add("for");   
            slist.add("from");
            slist.add("how");
            slist.add("that");  
            slist.add("about");
            slist.add("what");  
            slist.add("when");
            slist.add("where");
            slist.add("l"); 
            slist.add("who");   
            slist.add("will");
            slist.add("the");
            slist.add("this");
            slist.add("with");
            slist.add("was");
            slist.add("the");   
            slist.add("l40"); 
            slist.add("l300");
            slist.add("l200"); 
            slist.add("l100"); 
            slist.add("www");
        }
 

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
 
            String line    = value.toString().toLowerCase();
            String[] words = line.split("\\W+");
 
            for (int i=0; i < words.length; i++) {
                String word = words[i];
                if (word.length() < 3                   ||
                    !Character.isLetter(word.charAt(0)) ||
                    Character.isDigit(word.charAt(0))   ||
                    slist.contains(word)            ||
                    word.contains("_")) {
                    continue;
                } 
                String fileName =
                        ((FileSplit) context.getInputSplit()).getPath().getName();
                word_doc.set(word+"@"+fileName);
                context.write(word_doc, un);
            }
        }
    }
    public static class Reduce
    extends Reducer<Text, IntWritable, Text, IntWritable> {
 
        private static IntWritable nwr = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
 
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            nwr.set(sum);
            context.write(key, nwr);
        }
    }
}
