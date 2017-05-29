import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TFIDF_step4 {
    public static class Map
    extends Mapper<Text, Text, Text, Text> {
         
        private static int nfiles;
         
        public void setup(Context context) throws IOException {
            Configuration  conf = context.getConfiguration();
            FileSystem     fs   = FileSystem.get(conf);
            Path           pt   = new Path(conf.get("originalInputDir"));
            ContentSummary cs   = fs.getContentSummary(pt);
            nfiles           = (int)cs.getFileCount();
        }
 
        private final static Text word_doc = new Text();



        public void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {
 

            String[] n_N_m = value.toString().split(";");
            Integer n = Integer.parseInt(n_N_m[0]);
          Integer df = Integer.parseInt(n_N_m[2]);
 
            double tfii = n * Math.log10(nfiles/df);
            word_doc.set(key);
            String word = word_doc.toString() ;
            word_doc.set(word);
            context.write(word_doc, new Text(Double.toString(tfii)) );
        }
    }
}
