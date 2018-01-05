package cs.bigdata.Lab2.postprocessing;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;


public class Postprocessing extends Configured implements Tool {


    public int run(String[] args) throws Exception {

        if (args.length != 2) {
            System.out.println("Usage: [input] [output]");
            System.exit(-1);
        }


        // Création d'un job en lui fournissant la configuration et une description textuelle de la tâche
        Job job = Job.getInstance(getConf());
        job.setJobName("WordCount");


        // On précise les classes MyProgram, Map et Reduce
        job.setJarByClass(Postprocessing.class);
        job.setMapperClass(PostprocessingMapper.class);

        // Définition des types clé/valeur de notre problème
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(TextInputFormat.class);
        job.setOutputValueClass(TextOutputFormat.class);
        Path inputFilePath = new Path(args[0]);
        Path outputFilePath = new Path(args[1]);
        

     // On accepte une entrée recursive
        FileInputFormat.setInputDirRecursive(job, true);
        FileInputFormat.addInputPath(job, inputFilePath);
        FileOutputFormat.setOutputPath(job, outputFilePath);

        FileSystem fs = FileSystem.newInstance(getConf());

        if (fs.exists(outputFilePath)) {
            fs.delete(outputFilePath, true);
        }
        
        return job.waitForCompletion(true) ? 0: 1;
    }

    public static void main(String[] args) throws Exception {
        Postprocessing exempleDriver = new Postprocessing();
        int res = ToolRunner.run(exempleDriver, args);
        System.exit(res);

    }
}

