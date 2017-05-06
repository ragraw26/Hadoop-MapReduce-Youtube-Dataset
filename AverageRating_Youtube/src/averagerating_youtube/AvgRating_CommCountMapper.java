/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package averagerating_youtube;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author Rajat
 */
public class AvgRating_CommCountMapper extends Mapper<Object, Text, Text, AverageRating_CommentCountTuple> {

    // Our output key and value Writables
    private Text video_name = new Text();
    private float v_rate;
    private AverageRating_CommentCountTuple outTuple = new AverageRating_CommentCountTuple();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String[] fields = value.toString().split(",");
        String videoId = (fields[0]);
        if (!fields[6].isEmpty()) {
            this.v_rate = Float.parseFloat(fields[6]);
        } else {
            this.v_rate = 0;
        }
        video_name.set(videoId);
        outTuple.setComment_count(1);
        outTuple.setVideo_rating(this.v_rate);
        context.write(video_name, outTuple);

    }

}
