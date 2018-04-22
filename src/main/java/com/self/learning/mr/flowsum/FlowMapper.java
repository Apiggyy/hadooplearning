package com.self.learning.mr.flowsum;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable,Text,Text,FlowBean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = StringUtils.split(line, "\t");
        String msisdn = fields[1];
        long upFlow = Long.parseLong(fields[7]);
        long downFlow = Long.parseLong(fields[8]);

        context.write(new Text(msisdn),new FlowBean(msisdn, upFlow, downFlow));
    }
}
