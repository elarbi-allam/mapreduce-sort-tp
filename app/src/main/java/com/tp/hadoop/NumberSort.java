package com.tp.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NumberSort {

    // Mapper : utilise le nombre comme clé de tri
    public static class SortMapper
            extends Mapper<Object, Text, IntWritable, Text> {

        private final static Text EMPTY_TEXT = new Text("");
        private IntWritable number = new IntWritable();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            try {
                int num = Integer.parseInt(value.toString().trim());
                number.set(num);
                context.write(number, EMPTY_TEXT);
            } catch (NumberFormatException e) {
            }
        }
    }
    public static class SortReducer
            extends Reducer<IntWritable, Text, IntWritable, Text> {

        private final static Text EMPTY_TEXT = new Text("");

        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            context.write(key, EMPTY_TEXT);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Number Sort TP");

        job.setJarByClass(NumberSort.class);
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);

        // Définition des types de sortie
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        // Chemins d'entrée et de sortie HDFS
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}