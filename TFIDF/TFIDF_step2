import java.io.IOException;
import java.util.HashMap;
 
import org.apache.hadoop.io.Text;
 
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
  
public class TFIDF_step2 {
    public static class Map
    extends Mapper<Text, Text, Text, Text> {
        private static  Text wordn = new Text();
        private final static Text doc    = new Text();
        public void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] words = key.toString().split("@");
            String word  = words[0];
            doc.set(words[1]);
            String n = value.toString();
            wordn.set(word+";"+n);
            context.write(doc, wordn);
        }
    }
    public static class Reduce
    extends Reducer<Text, Text, Text, Text> {
 
        private static Text word_doc = new Text();
        private static Text n_N       = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {          
            String doc = key.toString();        
            int N = 0;
            HashMap<String, Integer> wordList = new HashMap<String, Integer>();
            wordList.clear();
            for (Text word_n: values) {
                String[] bits = word_n.toString().split(";");
                String word = bits[0];
                int n       = Integer.parseInt(bits[1]);
                wordList.put(word, n);
                N += n;
            }
            for (String word: wordList.keySet()) {
                word_doc.set(word+"@"+doc);
                n_N.set(wordList.get(word)+";"+N);
                context.write(word_doc,  n_N);
            }
        }
    }
}
