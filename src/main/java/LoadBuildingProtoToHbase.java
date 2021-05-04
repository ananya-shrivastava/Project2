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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class LoadBuildingProtoToHbase extends Configured implements Tool {

    public static class MyMapper extends Mapper<IntWritable, Text, ImmutableBytesWritable, Put> {
        private static final byte[] CF_BYTES1 = Bytes.toBytes("building_details");
        private static final byte[] QUAL_BYTES1 = Bytes.toBytes("building_code");
        private static final byte[] QUAL_BYTES2 = Bytes.toBytes("total_floors");
        private static final byte[] QUAL_BYTES3 = Bytes.toBytes("companies_in_the_building");
        private static final byte[] QUAL_BYTES4 = Bytes.toBytes("cafteria_code");

        @Override
        protected void map(IntWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (key.get() == 0 || value.getLength() == 0) {
                return;
            }
            System.out.println("in map");
            System.out.println("key: "+key+" value : "+ value);
            String[] strArr = value.toString().split("\n");
            String building_code = strArr[0].split(":")[1].trim();
            String total_floors = strArr[1].split(":")[1].trim();
            String companies_in_the_building = strArr[2].split(":")[1].trim();
            String cafeteria_code = strArr[3].split(":")[1].trim();

            byte[] rowKey = Bytes.toBytes(String.valueOf(key));
            Put put = new Put(rowKey);
            put.addColumn(CF_BYTES1, QUAL_BYTES1, Bytes.toBytes(building_code));
            put.addColumn(CF_BYTES1, QUAL_BYTES2, Bytes.toBytes(total_floors));
            put.addColumn(CF_BYTES1, QUAL_BYTES3, Bytes.toBytes(companies_in_the_building));
            put.addColumn(CF_BYTES1, QUAL_BYTES4, Bytes.toBytes(cafeteria_code));
            context.write(new ImmutableBytesWritable(rowKey), put);
        }

    }


    public int run(final String[] args) throws Exception {
        Job job = new Job();
        job.setJobName("BadRequestsByIp");
        job.setJarByClass(MyMapper.class);
        job.setMapperClass(MyMapper.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        Configuration conf = HBaseConfiguration.create(getConf());
        setConf(conf);
        String tableNameString = "building";
        try (Connection connection = ConnectionFactory.createConnection(conf)) {
            TableName tableName = TableName.valueOf(tableNameString);
            Table table = connection.getTable(tableName);
            RegionLocator regionLocator = connection.getRegionLocator(tableName);
            HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator);
        }

        String outPath = "hdfs://localhost:8020/BuildingProtoHbase";
        String input = "hdfs://localhost:8020/ProtoFiles/building.seq";
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
        int exitCode = ToolRunner.run(HBaseConfiguration.create(), new LoadBuildingProtoToHbase(), args);
        System.exit(exitCode);
    }
}