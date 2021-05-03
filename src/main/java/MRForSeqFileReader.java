import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MRForSeqFileReader extends Configured implements Tool{
    public static class SeqFileMapper extends Mapper<IntWritable, Text, IntWritable, Text>{
        public void map(IntWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            context.write(key, value);
        }
    }
    public static void main(String[] args)  throws Exception{
        int flag = ToolRunner.run(new MRForSeqFileReader(), args);
        System.exit(flag);

    }
    @Override
    public int run(String[] args) throws Exception {
        String inPath ="hdfs://localhost:8020/ProtoFiles/building.seq";
        String outPath = "hdfs://localhost:8020/SeqOuptut";
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "seqfileread");
        job.setJarByClass(MRForSeqFileReader.class);
        job.setMapperClass(SeqFileMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(inPath));
        FileOutputFormat.setOutputPath(job, new Path(outPath));

        int returnFlag = job.waitForCompletion(true) ? 0 : 1;
        return returnFlag;
    }
}