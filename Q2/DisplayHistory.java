import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import java.io.*;

public class DisplayHistory {

    public static void main(String[] args) throws IOException {
            
        Path filename = new Path("args");
        Configuration confing = new Configuration();
        FileSystem fs = FileSystem.get(confing);
        FSDataInputStream inStream = fs.open(filename);    
        int lines = 0;
        try{
            
            InputStreamReader input = new InputStreamReader(inStream);
            BufferedReader buffer = new BufferedReader(input);        
            String cline = buffer.readLine();
            while (cline !=null){
                lines++;
                if (lines >= 23){
                    // Process of the current line
                    if (cline.length() > 0){
                        String station = cline.substring(13, 42);
                        String fips = cline.substring(43,45);
                        String altitude = cline.substring(74,81);
                        System.out.println(String.format("Station: %s     FIPS: %s       US Altitude: %s" ,station,fips,altitude));
                        cline = buffer.readLine();
                        continue;
                    }
                    cline = buffer.readLine();
                    continue;
                }
                cline = buffer.readLine();
                continue;
            }
        }
        finally{
            System.out.println(String.format("There are %s lines", lines));
            inStream.close();
            fs.close();
        }
    }
}
