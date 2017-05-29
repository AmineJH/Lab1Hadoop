import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class Top20 {

    public static void main(String[] args) throws IOException {    
        Path filename = new Path("/home/cloudera/workspace/TFIDF/TFIDF4/part-m-00000");
        Configuration confing = new Configuration();
        FileSystem fs = FileSystem.get(confing);
        FSDataInputStream inStream = fs.open(filename);    
         Map<Text, DoubleWritable> countMap = new HashMap<>();
        try{            
            InputStreamReader input = new InputStreamReader(inStream);
            BufferedReader buffer = new BufferedReader(input);        
            String cline = buffer.readLine();
            while (cline !=null){
                String[] part1 = cline.split("(?<=\\D)(?=\\d)");
                 countMap.put(new Text(part1[0]), new DoubleWritable(Double.parseDouble(part1[1])));
                cline = buffer.readLine();
                continue;
            }
        }
        finally{
            inStream.close();
            fs.close();
        }        
        Map<Text, DoubleWritable> sortedMap = MiscUtils.sortByValues(countMap);
        int counter=0;
        for (Map.Entry<Text, DoubleWritable> entry : sortedMap.entrySet())
        {
            if (counter++ == 20) {
                break;
            }
            System.out.println(entry.getKey() + " " + entry.getValue());
        }
    }
}
