package com.hp;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HPMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException{
		
		String [] tokens = value.toString().split(",");
		
		String unit = tokens[0];
		String timestamp = tokens[1];
		String tag = tokens[2];
		double sensorvalue = Double.parseDouble(tokens[3]);
		int flagvalue = Integer.parseInt(tokens[4]);
		//we have taken tagvalue as a string because we need to write null into it
		String tagvalue;
		if(flagvalue>0){
			tagvalue = String.valueOf(sensorvalue);
		}
		else{
			tagvalue = "null";
		}
		context.write(new Text(unit+","+timestamp),new Text(tag + "," + tagvalue));
	}
}
