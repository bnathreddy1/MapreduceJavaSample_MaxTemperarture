
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

//The MaxTempMapper has 4 parameters.
//The first two parameters dictate the inputs to the Mapper class. The Key and Value pair
//The third and fourth parameters tells us the output from Mapper class Key and Value 

//First parameter LongWritable = InputKey
//Second parameter Text = InputValue
//Third parameter Text = OutputKey
//Fourth parameter IntWritable = OutputValue

public class MaxTempMapper extends Mapper<LongWritable, Text, Text, IntWritable> 
{
		  
	 /**
     * @method map
     * <p>This method takes the input as text data type and and tokenizes input
     * by taking whitespace as delimiter. The first token goes year and second token is temperature,
     * this is repeated till last token. Now key value pair is made and passed to reducer.                                             
     
     */
    
    //Defining a local variable k of type Text  
    Text k= new Text(); 

	 @Override
     public void map(LongWritable key, Text value, Context context)
     throws IOException, InterruptedException {

             //Converting the record (single line) to String and storing it in a String variable line
             String line = value.toString(); 

             //StringTokenizer is breaking the record (line) according to the delimiter whitespace
             StringTokenizer tokenizer = new StringTokenizer(line," "); 

             //Iterating through all the tokens and forming the key value pair
             while (tokenizer.hasMoreTokens()) { 

		             //The first token is going in year variable of type string
		             String year= tokenizer.nextToken();
		             k.set(year);
		
		             //Takes next token and removes all the whitespaces around it and then stores it in the string variable called temp
		             String temp= tokenizer.nextToken().trim();
		
		             //Converts string temp into integer v           
		             int v = Integer.parseInt(temp); 
		
		             //Sending to output collector which inturn passes the same to reducer
		             context.write(k,new IntWritable(v)); 
         } 
     } 
 } 