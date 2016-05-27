import java.io.IOException;

import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class LinkMapper extends
    Mapper<LongWritable, Text, NullWritable, Text> {
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	
  	 String line = value.toString(); 
	 String columns[]=line.split("\n");
	 
	 Pattern p1 = Pattern.compile("#index[0-9]");
	 Matcher m1 = p1.matcher(line); 
	 
		 if(m1.find())
		 {
			 
			 String str[] = columns[4].split("#index");
			 String index = str[1];
			 
			 Pattern p2 = Pattern.compile("#%[0-9]");
			 Matcher m2 = p2.matcher(line); 
	 
			 for(int i=5;i<columns.length-1;i++)
			 {
			  
				 if(m2.find())
				 {
				 
					 String ref=columns[i].substring(2);
				 
					 Pattern p = Pattern.compile("#*[A-z0-9\\s+]*");
					 Matcher m = p.matcher(line);
			  
					 if(m.find())
					 {
				 
						 String title = columns[0].substring(2);
						 //System.out.println(title);
				 
						 //System.out.println("*******************");
						 context.write(NullWritable.get(),new Text(index.trim()+"\t"+ref.trim()+"\t"+title));
					 }
			 
				 }
			 
			 }
		 
		 }	 
		 
	 }
 }
  

