import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;

public class SetAndGetProtoObject {

    private static final String EMPLOYEE_CSV_FILE_PATH = "/Users/ananyashrivastava/IdeaProjects/EmployeeCsvToProto/src/main/resources/employee.csv";
    private static final String BUILDING_CSV_FILE_PATH = "/Users/ananyashrivastava/IdeaProjects/EmployeeCsvToProto/src/main/resources/building.csv";

    public static void main(String args[]) throws IOException, Exception {

        //setting employee proto objects
        Reader reader = Files.newBufferedReader(Paths.get(EMPLOYEE_CSV_FILE_PATH));
        boolean columnRead = false;
        Iterable<CSVRecord> records = CSVFormat.DEFAULT.parse(reader);
        ArrayList<EmployeeOuterClass.Employee.Builder> employeeList = new ArrayList<EmployeeOuterClass.Employee.Builder>();
        for (CSVRecord record : records) {

            if (columnRead) {
                EmployeeOuterClass.Employee.Builder employee = EmployeeOuterClass.Employee.newBuilder();
                employee.setName(record.get(0));
                employee.setEmployeeId(Integer.parseInt(record.get(1)));
                employee.setBuildingCode(Integer.parseInt(record.get(2)));
                String floor_name = record.get(3);
                EmployeeOuterClass.Employee.FloorNumber floor = null;
                if (floor_name.equals("GROUND")) {
                    floor = EmployeeOuterClass.Employee.FloorNumber.GROUND;
                } else if (floor_name.equals("FIRST")) {
                    floor = EmployeeOuterClass.Employee.FloorNumber.FIRST;
                } else if (floor_name.equals("SECOND")) {
                    floor = EmployeeOuterClass.Employee.FloorNumber.SECOND;
                }
                employee.setFloorNumber(floor);
                employee.setSalary(Integer.parseInt(record.get(4)));
                employee.setDepartment(record.get(5));
                employeeList.add(employee);
            }
            columnRead = true;
        }

        //setting building proto objects
        ArrayList<BuildingOuterClass.Building.Builder> buildingList = new ArrayList<BuildingOuterClass.Building.Builder>();
        reader = Files.newBufferedReader(Paths.get(BUILDING_CSV_FILE_PATH));
        columnRead = false;
        records = CSVFormat.DEFAULT.parse(reader);
        for (CSVRecord record : records) {

            if (columnRead) {
                BuildingOuterClass.Building.Builder building = BuildingOuterClass.Building.newBuilder();
                building.setBuildingCode(Integer.parseInt(record.get(0)));
                building.setTotalFloors(Integer.parseInt(record.get(1)));
                building.setCompaniesInTheBuilding(Integer.parseInt(record.get(2)));
                building.setCafeteriaCode(record.get(3));
                buildingList.add(building);
            }
            columnRead = true;
        }

        //Writing Proto Objects to Hdfs in Sequence files
        String uri1 = "hdfs://localhost:8020/ProtoFiles/building.seq";
        String uri2 = "hdfs://localhost:8020/ProtoFiles/employee.seq";
        WriteProtoToHdfs.writeProtoObjectsToHdfs(employeeList, buildingList, uri2, "employee");
        WriteProtoToHdfs.writeProtoObjectsToHdfs(employeeList, buildingList, uri1, "building");

//      printing Employee and Building Proto Objects
//        for (EmployeeOuterClass.Employee.Builder emp : employeeList)
//        {
//            System.out.println(emp);
//        }
//        for (BuildingOuterClass.Building.Builder build : buildingList)
//        {
//            System.out.println(build);
//        }


    }

}

