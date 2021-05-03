import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayFile;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class WriteProtoToHdfs {

    public static void writeProtoObjectsToHdfs(ArrayList<EmployeeOuterClass.Employee.Builder> employeeList, ArrayList<BuildingOuterClass.Building.Builder> buildingList, String uri,String protoObject) throws Exception
    {
        Configuration config = new Configuration();
        config.addResource(new Path("/opt/homebrew/Cellar/hadoop/3.3.0/libexec/etc/hadoop/core-site.xml"));
        config.addResource(new Path("/opt/homebrew/Cellar/hadoop/3.3.0/libexec/etc/hadoop/hdfs-site.xml"));
        FileSystem fs = FileSystem.get(config);

        SequenceFile.Writer writer = null;
        SequenceFile.Reader reader = null;
        try {
            Path path = new Path(uri);
            IntWritable key = new IntWritable();
            Text value = new Text();
            int id = 0;

            if (!fs.exists(path)) {
                writer = SequenceFile.createWriter(config, SequenceFile.Writer.file(path), SequenceFile.Writer.keyClass(key.getClass()),
                        ArrayFile.Writer.valueClass(value.getClass()));
                System.out.println("file created");

                if(protoObject.equals("employee")) {
                    for (EmployeeOuterClass.Employee.Builder e : employeeList) {
                        writer.append(new IntWritable((++id)), new Text(e.toString()));
                    }
                }
                else {
                    for (BuildingOuterClass.Building.Builder e : buildingList) {
                        writer.append(new IntWritable((++id)), new Text(e.toString()));
                    }
                }
                writer.close();
            } else {
                //logger.info(path + " already exists.");
            }

            /** Create a SequenceFile Reader object.*/
            reader = new SequenceFile.Reader(config, SequenceFile.Reader.file(path));

            System.out.println("Reading " + protoObject);
            while (reader.next(key, value)) {
                System.out.println(key + "\t" + value);
            }

            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(writer);
            IOUtils.closeStream(reader);
        }


    }
}
