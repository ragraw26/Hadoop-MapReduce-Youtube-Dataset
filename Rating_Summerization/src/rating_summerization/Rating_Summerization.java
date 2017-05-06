/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rating_summerization;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author Rajat
 */
public class Rating_Summerization {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException {
        try {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "VideoMinMaxRating");
            job.setJarByClass(Rating_Summerization.class);
            job.setMapperClass(Summ_Mapper.class);
            job.setCombinerClass(Summ_Reducer.class);
            job.setReducerClass(Summ_Reducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(MinMaxCountTuple.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (InterruptedException | ClassNotFoundException ex) {
            Logger.getLogger(Rating_Summerization.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

}
