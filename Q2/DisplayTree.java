import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class DisplayTree {
    public static void main(String[] args) throws IOException {
        
        Path filename = new Path("/home/cloudera/workspace/Q2/IO/Input/arbres.csv");
        Configuration confing = new Configuration();
        FileSystem fs = FileSystem.get(confing);
        FSDataInputStream inStream = fs.open(filename);    
        try{
            InputStreamReader reader = new InputStreamReader(inStream);
            BufferedReader buffer = new BufferedReader(reader);        
            String line = buffer.readLine();
            while (line !=null){
                String[] words = line.split(";");
                System.out.println(words[5] + "  " + words[6]);
                line = buffer.readLine();
            }
        }
        finally{
            inStream.close();
            fs.close();
        }
    }
}
