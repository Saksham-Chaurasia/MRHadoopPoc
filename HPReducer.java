package com.hp;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class HPReducer extends Reducer<Text, Text, Text, Text>{

	public void reduce(Text key, Iterable<Text> value, Context context)throws IOException, InterruptedException{
		String[] tagvalues= new String[70];
		
		
		
		for(Text values: value){
			String [] tokens = values.toString().split(",");
			
			for(int i=0;i<70; i++){
				if(i==(Integer.parseInt(tokens[0].substring(3)))-1){
					tagvalues[i] =tokens[1];
					break;
				}
			}
		}
		
		StringBuilder sb = new StringBuilder();
		for(int i =0; i<tagvalues.length; i++){
			sb.append(tagvalues[i]);
			if(i<tagvalues.length-1){
				sb.append(",");
			}
		}
	
		String result = sb.toString();
		
		
		context.write(new Text(key), new Text(result));
		
	}
}
