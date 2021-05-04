import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class LoadEmployeeProtoToHbase extends Configured implements Tool {

    public static class EmployeeMapper extends Mapper<IntWritable, ImmutableBytesWritable, ImmutableBytesWritable, Put> {
        private static final byte[] CF_BYTES1 = Bytes.toBytes("employee_details");
        private static final byte[] QUAL_BYTES1 = Bytes.toBytes("employee_qual");

        @Override
        protected void map(IntWritable key, ImmutableBytesWritable value, Context context) throws IOException, InterruptedException {
            if (key.get() == 0 || value.getLength() == 0) {
                return;
            }

            EmployeeOuterClass.Employee.Builder employee =
                    EmployeeOuterClass.Employee.newBuilder().mergeFrom(value.get());
            byte[] rowKey = Bytes.toBytes(String.valueOf(key));
            Put put = new Put(rowKey);

            put.addColumn(CF_BYTES1,QUAL_BYTES1, employee.build().toByteArray());
            context.write(new ImmutableBytesWritable(rowKey), put);
        }

    }

    public int run(final String[] args) throws Exception {
        Job job = new Job();
        job.setJobName("LoadEmployeeProtoToHbase");
        job.setJarByClass(EmployeeMapper.class);
        job.setMapperClass(EmployeeMapper.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        Configuration conf = HBaseConfiguration.create(getConf());
        setConf(conf);
        String tableNameString = "employee";
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
            TableName tableName = TableName.valueOf(tableNameString);
            Table table = connection.getTable(tableName);
            RegionLocator regionLocator = connection.getRegionLocator(tableName);
            HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator);
        }

        String outPath = "hdfs://localhost:8020/EmployeeProtoHbase";
        String input = "hdfs://localhost:8020/ProtoFiles/employee.seq";
        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(outPath));

        boolean success = job.waitForCompletion(true);
        doBulkLoad(tableNameString, new Path(outPath));
        return success ? 0 : 1;
    }

    private void doBulkLoad(String tableNameString, Path tmpPath) throws Exception {
        LoadIncrementalHFiles loader = new LoadIncrementalHFiles(getConf());
        try (Connection connection = ConnectionFactory.createConnection(getConf()); Admin admin = connection.getAdmin()) {
            TableName tableName = TableName.valueOf(tableNameString);
            Table table = connection.getTable(tableName);
            RegionLocator regionLocator = connection.getRegionLocator(tableName);
            loader.doBulkLoad(tmpPath, admin, table, regionLocator);
        }
    }

    public static void main(final String[] args) throws Exception {
        int exitCode = ToolRunner.run(HBaseConfiguration.create(), new LoadEmployeeProtoToHbase(), args);
        System.exit(exitCode);
    }
}