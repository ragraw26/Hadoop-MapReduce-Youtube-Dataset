/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rating_summerization;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author Rajat
 */
class Summ_Reducer extends Reducer<Text, MinMaxCountTuple, Text, MinMaxCountTuple> {

    private MinMaxCountTuple result = new MinMaxCountTuple();

    @Override
    protected void reduce(Text key, Iterable<MinMaxCountTuple> values, Context context) throws IOException, InterruptedException {
        // Initialize our result
        result.setAverageRating(0);
        result.setTotalRating(0);
        result.setTotalComment(0);
        int sum = 0;

        for (MinMaxCountTuple val : values) {

            if (result.getAverageRating()== 0 || val.getAverageRating() < result.getAverageRating()) {
                result.setAverageRating(val.getAverageRating());
            }

            if (result.getTotalRating()== 0
                    || val.getTotalRating() > (result.getTotalRating())) {
                result.setTotalRating(val.getTotalRating());
            }

            sum += val.getTotalComment();

        }
        result.setTotalComment(sum);
        context.write(key, result);
    }

}
