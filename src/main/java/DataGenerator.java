import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.*;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class DataGenerator {
  private static final Session session = new Session("127.0.0.1", 6667, "root", "root");
  private static final int TIMESERIES_NUM = 1000;
  private static final int DATA_NUM = 100000;

  public static void main(String[] args) throws Exception {
    session.open(false);
    session.setStorageGroup("root.test");
    createTimeseries();
    generateData();
    session.executeNonQueryStatement("flush");
    session.close();
  }

  static void createTimeseries() throws StatementExecutionException, IoTDBConnectionException {
    for (int i = 0; i < TIMESERIES_NUM; ++i) {
      System.out.println("Creating TimeSeries" + i);
      session.createTimeseries(
              "root.test.device.s" + String.valueOf(i),
              TSDataType.DOUBLE,
              TSEncoding.PLAIN,
              CompressionType.SNAPPY);
    }
  }

  static void generateData() throws Exception {
    Random r = new Random();
    List<MeasurementSchema> schemaList = new ArrayList<>();
    for (int s = 0; s < TIMESERIES_NUM; ++s) {
      schemaList.add(new MeasurementSchema("s" + s, TSDataType.DOUBLE));
    }
    Tablet tablet = new Tablet("root.test.device", schemaList, 2000);
    long timestamp = 0;
    int rowIdx = 0;
    for (int j = 0; j < DATA_NUM; ++j) {
      rowIdx = tablet.rowSize++;
      timestamp++;
      tablet.addTimestamp(rowIdx, timestamp);
      for(int s = 0; s < TIMESERIES_NUM; ++s) {
        tablet.addValue(schemaList.get(s).getMeasurementId(), rowIdx, r.nextDouble());
      }
      if (tablet.rowSize == tablet.getMaxRowNumber()) {
        session.insertTablet(tablet, true);
        tablet.reset();
      }
    }
    if (tablet.rowSize != 0) {
      session.insertTablet(tablet, true);
    }
  }
}