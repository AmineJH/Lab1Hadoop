import java.io.IOException;
import java.util.HashMap;
 
import org.apache.hadoop.io.Text;
 
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
 
public class TFIDF_step3 {
    public static class Map
    extends Mapper<Text, Text, Text, Text> {
 
        private final static Text wd      = new Text();
        private static       Text doc_n_N_1 = new Text();
        @Override
        public void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
 
            String[] word_doc = key.toString().split("@");
            wd.set(word_doc[0]);
            String doc = word_doc[1];
            String[] n_N = value.toString().split(";");
            String n = n_N[0];
            String N = n_N[1]; 
            doc_n_N_1.set(doc +";"+ n +";"+ N +";"+ 1);
            context.write(wd, doc_n_N_1);
        }
    }
    public static class Reduce
    extends Reducer<Text, Text, Text, Text> {
        private static Text word_doc = new Text();
        private static Text n_N_df   = new Text();
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            HashMap<String, Integer> word_docList = new HashMap<String, Integer>();
            HashMap<String, Integer> docList      = new HashMap<String, Integer>();
            HashMap<String, Integer> wordList     = new HashMap<String, Integer>();
            word_docList.clear();
            docList.clear();
            wordList.clear();
            String word = key.toString();
            for (Text value: values) {
                String[] doc_n_N_count = value.toString().split(";");
                String  doc    = doc_n_N_count[0];
                Integer n      = Integer.parseInt(doc_n_N_count[1]);
                Integer N      = Integer.parseInt(doc_n_N_count[2]);
                Integer count  = Integer.parseInt(doc_n_N_count[3]);
 
                if (!docList.containsKey(doc)) {
                    docList.put(doc,N);
                } else {
                    if (N != docList.get(doc)) {
                        System.out.println("N != docList.get(doc)"+
                                ": N="+N+
                                "; doc="+doc+
                                "; docList.get(doc)="+docList.get(doc));
                        System.exit(-1);
                    }
                }
                 
                if (!word_docList.containsKey(word+"@"+doc)) {
                    word_docList.put(word+"@"+doc,n);
                } else {
                    if (n != word_docList.get(word+"@"+doc)) {
                        System.out.println("n != word_docList.get(word+\"@\"+doc)"+
                                ": n="+n+
                                "; (word+\"@\"+doc="+word+"@"+doc+
                                "; word_docList="+word_docList.get(word+"@"+doc));
                        System.exit(-1);
                    }
                }
                 
                if (!wordList.containsKey(word)) {
                    wordList.put(word, count);
                } else {
                    Integer df = wordList.get(word);
                    df += count;
                    wordList.put(word, df);
                }
            }
 
            for (String WordDoc: word_docList.keySet()) {
                String[] Word_Doc = WordDoc.split("@");
                String Word       = Word_Doc[0];
                String Doc        = Word_Doc[1];
                Integer little_en = word_docList.get(WordDoc);
                Integer big_en    = docList.get(Doc);
                Integer df        = wordList.get(Word);
                word_doc.set(WordDoc);
                n_N_df.set(little_en+";"+big_en+";"+df);
                context.write(word_doc,  n_N_df);
            }
        }
    }
}
