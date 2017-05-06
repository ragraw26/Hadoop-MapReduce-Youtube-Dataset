/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rating_summerization;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author Rajat
 */
class Summ_Mapper extends Mapper<Object, Text, Text, MinMaxCountTuple> {

    private Text video_ID = new Text();
    private MinMaxCountTuple outTuple = new MinMaxCountTuple();

    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String[] input = value.toString().split(",");

        video_ID.set(input[0]);
        outTuple.setTotalRating(Float.valueOf(input[7]));
        outTuple.setAverageRating(Float.valueOf(input[6]));
        outTuple.setTotalComment(Float.valueOf(input[8]));

        context.write(video_ID, outTuple);
    }

}
