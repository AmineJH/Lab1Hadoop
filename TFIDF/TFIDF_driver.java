
Conversation ouverte. 1 message non lu.

Aller au contenu
Utiliser Gmail avec un lecteur d'écran
Recherche



Gmail
NOUVEAU MESSAGE
Libellés
Boîte de réception (1 158)
Messages suivis
Messages envoyés
Brouillons (16)
Unwanted (45)
Plus 
Hangouts

 
 
 
  Plus 
1 sur 1 796  
 
Tout imprimer Dans une nouvelle fenêtre
(aucun objet) 
Boîte de réception
x 

Amine Jai Hokimi <amine.jaihokimi@gmail.com>
Pièces jointes 13:29 (Il y a 3 minutes)

À moi 
6 pièces jointes 
 
	
Cliquez ici pour répondre au message ou le transférer
0,36 Go (2 %) utilisés sur 15 Go
Gérer
Conditions d'utilisation - Confidentialité
Dernière activité sur le compte : Il y a 2 minutes
Détails

import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
public class TFIDF_driver extends Configured implements Tool{
    public static void main(String[] args) throws Exception {
 
        System.out.println("driver main() args: " + Arrays.toString(args));
        int res = ToolRunner.run(new Configuration(), new TFIDF_driver(), args);
 
        System.exit(res);
    }
 
    public int run(String[] args) throws Exception { 
        Configuration cf = new Configuration();
        int step = 1;
        Job job1 = Job.getInstance(cf);
        job1.setJobName("Job" + step);
        System.out.println("job: " + job1.getJobName().toString());
        job1.setJarByClass(TFIDF_driver.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        job1.setMapperClass(TFIDF_step1.Map.class);
        job1.setReducerClass(TFIDF_step1.Reduce.class);
 
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
 
        cf.set("inputDir", args[0]);
        cf.set("originalInputDir", args[0]);
        cf.set("outputDir", job1.getJobName());
 
        FileInputFormat.addInputPath(job1, new Path(cf.get("inputDir")));
 
        FileSystem fs1 = FileSystem.get(cf);
        if (fs1.exists(new Path(cf.get("outputDir"))))
            fs1.delete(new Path(cf.get("outputDir")), true);
        FileOutputFormat.setOutputPath(job1, new Path(cf.get("outputDir")));
 
        for (Path inputPath: FileInputFormat.getInputPaths(job1))
            System.out.println("input  path " + inputPath.toString());
        System.out.println("output path " +
                FileOutputFormat.getOutputPath(job1).toString());
        job1.waitForCompletion(true);
        cf.set("inputDir", cf.get("outputDir"));
        step++;
 
        Job job2 = Job.getInstance(cf);
        job2.setJobName("Job" + step);
        System.out.println("job : " + job2.getJobName().toString());
        job2.setJarByClass(TFIDF_driver.class);
 
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
         job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
 
        job2.setMapperClass(TFIDF_step2.Map.class);
        job2.setReducerClass(TFIDF_step2.Reduce.class);
 
        cf.set("key.value.separator.in.input.line", "\t");
        job2.setInputFormatClass(KeyValueTextInputFormat.class);
 
        job2.setOutputFormatClass(TextOutputFormat.class);
 
        cf.set("outputDir", job2.getJobName());
 
        FileInputFormat.addInputPath(job2, new Path(cf.get("inputDir")));
 
        FileSystem fs2 = FileSystem.get(cf);
        if (fs2.exists(new Path(cf.get("outputDir"))))
            fs2.delete(new Path(cf.get("outputDir")), true);
        FileOutputFormat.setOutputPath(job2, new Path(cf.get("outputDir")));
 
        for (Path inputPath: FileInputFormat.getInputPaths(job2))
            System.out.println("input  path " + inputPath.toString());
        System.out.println("output path " +
                FileOutputFormat.getOutputPath(job2).toString());
 
        job2.waitForCompletion(true);

        cf.set("inputDir", cf.get("outputDir"));
        step++;
 
        Job job3 = Job.getInstance(cf);
        job3.setJobName("Job" + step);
        System.out.println("job : " + job3.getJobName().toString());
        job3.setJarByClass(TFIDF_driver.class);
 
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
 
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
 
        job3.setMapperClass(TFIDF_step3.Map.class);
        job3.setReducerClass(TFIDF_step3.Reduce.class);
 
        cf.set("key.value.separator.in.input.line", "\t");
        job3.setInputFormatClass(KeyValueTextInputFormat.class);
 
        job3.setOutputFormatClass(TextOutputFormat.class);
         cf.set("outputDir", job3.getJobName());
 
        FileInputFormat.addInputPath(job3, new Path(cf.get("inputDir")));
 
        FileSystem fs3 = FileSystem.get(cf);
        if (fs3.exists(new Path(cf.get("outputDir"))))
            fs3.delete(new Path(cf.get("outputDir")), true);
        FileOutputFormat.setOutputPath(job3, new Path(cf.get("outputDir")));
 
        for (Path inputPath: FileInputFormat.getInputPaths(job3))
            System.out.println("input  path " + inputPath.toString());
        System.out.println("output path " +
                FileOutputFormat.getOutputPath(job3).toString());
 
        job3.waitForCompletion(true);
        cf.set("inputDir", cf.get("outputDir"));
        step++;
 
        Job job4 = Job.getInstance(cf);
        job4.setJobName("Job" + step);
        System.out.println("job : " + job4.getJobName().toString());
        job4.setJarByClass(TFIDF_driver.class);
 
        job4.setMapOutputKeyClass(Text.class);
        job4.setMapOutputValueClass(Text.class);
 
        job4.setNumReduceTasks(0);
 
        job4.setMapperClass(TFIDF_step4.Map.class);
 
        cf.set("key.value.separator.in.input.line", "\t");
        job4.setInputFormatClass(KeyValueTextInputFormat.class);
 
        cf.set("outputDir", job4.getJobName());
 
        FileInputFormat.addInputPath(job4, new Path(cf.get("inputDir")));
 
        FileSystem fs4 = FileSystem.get(cf);
        if (fs4.exists(new Path(cf.get("outputDir"))))
            fs4.delete(new Path(cf.get("outputDir")), true);
        FileOutputFormat.setOutputPath(job4, new Path(cf.get("outputDir")));
        for (Path inputPath: FileInputFormat.getInputPaths(job4))
            System.out.println("input  path " + inputPath.toString());
        System.out.println("output path " +
                FileOutputFormat.getOutputPath(job4).toString());

        job4.waitForCompletion(true); 
        return 0;
    }
} 
TFIDF_driver.java
Ouvrir avec
Affichage de TFIDF_driver.java en cours...
