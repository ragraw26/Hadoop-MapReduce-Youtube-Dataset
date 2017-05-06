/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package averagerating_youtube;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author Rajat
 */
public class AverageRating_Youtube extends Configured implements Tool {

    /**
     * @param args the command line arguments
     */
    @Override
    public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        job.setJobName("AverageRating_Youtube");

        job.setJarByClass(AverageRating_Youtube.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(AvgRating_CommCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(AverageRating_CommentCountTuple.class);
        job.setCombinerClass(AvgRating_CommCountCombiner.class);
        job.setReducerClass(AvgRating_CommCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(AverageRating_CommentCountTuple.class);
        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        // TODO code application logic here
        int exitCode = ToolRunner.run(new Configuration(),
                new AverageRating_Youtube(), args);
        System.exit(exitCode);
    }

}
