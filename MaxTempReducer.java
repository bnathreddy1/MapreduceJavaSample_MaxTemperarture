
import java.io.IOException; 
import java.util.StringTokenizer; 
  
/*
 * All org.apache.hadoop packages can be imported using the jar present in lib 
 * directory of this java project.
 */
 
 
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * @method reduce
 * <p>This method takes the input as key and list of values pair from mapper, it does aggregation
 * based on keys and produces the final output.                                               
 * @method_arguments key, values, output, reporter  
 * @return void
 */
          


public class MaxTempReducer  extends Reducer<Text, IntWritable, Text, IntWritable> { 
	
	 @Override
     public void reduce(Text key, Iterable<IntWritable> values, Context context)
     throws IOException, InterruptedException {
         /*
          * Iterates through all the values available with a key and if the integer variable temperature
          * is greater than maxtemp, then it becomes the maxtemp
          */

       //Defining a local variable maxtemp of type int
         int maxtemp=0;
         for(IntWritable it : values) { 

         //Defining a local variable temperature of type int which is taking all the temperature
         int temperature= it.get();
             if(maxtemp<temperature)
             {
                 maxtemp =temperature;
             }
         } 
          
         //Finally the output is collected as the year and maximum temperature corresponding to that year
         context.write(key, new IntWritable(maxtemp)); 
     } 

 } 
  
