package com.self.learning.mr.flowsumarea;

import com.self.learning.mr.flowsum.FlowBean;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowSumArea {

    @SuppressWarnings("Duplicates")
    private static class FlowSumAreaMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
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

    private static class FlowSumAreaReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
        @Override
        protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException,
                InterruptedException {
            long totalUpFlow = 0;
            long totalDownFlow = 0;
            for (FlowBean value : values) {
                totalUpFlow += value.getUpFlow();
                totalDownFlow += value.getDownFlow();
            }
            context.write(key, new FlowBean(key.toString(),totalUpFlow,totalDownFlow));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(FlowSumArea.class);
        job.setMapperClass(FlowSumAreaMapper.class);
        job.setReducerClass(FlowSumAreaReducer.class);
        job.setPartitionerClass(FlowAreaPartitioner.class);
        job.setNumReduceTasks(6);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
