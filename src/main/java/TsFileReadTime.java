import org.apache.iotdb.tsfile.*;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.read.ReadOnlyTsFile;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.BinaryExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.TimeFilter;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import java.util.*;

public class TsFileReadTime {
  static String filePath = "/data/iotdb/sequence/root.test/0/1612249694311-1-0.tsfile";
  public static void main(String[] args) throws Exception{
    int dataCount = 100;
    if (args.length > 0) {
      dataCount = Integer.valueOf(args[0]);
    }
    try(TsFileSequenceReader reader = new TsFileSequenceReader(filePath);
        ReadOnlyTsFile readOnlyTsFile = new ReadOnlyTsFile(reader)) {
      ArrayList<Path> paths = new ArrayList<>();
      Set<Integer> idxSet = new HashSet<>();
      Random r = new Random();
      while (idxSet.size() < dataCount) {
        int value = r.nextInt() % 1000;
        value = value < 0 ? -value : value;
        idxSet.add(value);
      }
      List<Integer> measurementIdx = new ArrayList<>();
      for(Integer idx : idxSet) {
        measurementIdx.add(idx);
      }
      Collections.sort(measurementIdx);
      float estimatedCost = 0;
      for(int i = 0; i < measurementIdx.size() - 1; ++i) {
        if (measurementIdx.get(i+1) - measurementIdx.get(i) > 1) {
          estimatedCost += CostEstimator.getSeekCost((long)(measurementIdx.get(i+1) - measurementIdx.get(i) - 1) * 161178l);
        }
      }
      System.out.println(measurementIdx.toString());
      Set<String> sensorSet = new HashSet<>();
      for(Integer idx : measurementIdx) {
        paths.add(new Path("root.test.device", "s" + idx));
        sensorSet.add("s" + idx);
      }

      long startTime = System.currentTimeMillis();
      List<TimeseriesMetadata> metadatas = reader.readTimeseriesMetadata("root.test.device", sensorSet);
      for(TimeseriesMetadata metadata : metadatas) {

      }
      long metadataReadTime = System.currentTimeMillis() - startTime;
      System.out.println("Metadata read time: " + metadataReadTime + " ms");
      // IExpression timeFilter = new GlobalTimeExpression(TimeFilter.ltEq(10000l));
      QueryExpression queryExpression = QueryExpression.create(paths, null);
      QueryDataSet dataSet = readOnlyTsFile.query(queryExpression);
      startTime = System.currentTimeMillis();
      while(dataSet.hasNext()) {
        dataSet.next();
      }
      long lastTime = System.currentTimeMillis() - startTime;
      System.out.println("Estimated seek time: " + (estimatedCost * 5.0f) + " ms");
      System.out.println("Estimated read time: " + ((161178.0f / (80645.0 / 1.1)) * (dataCount * 5)) + " ms");
      System.out.println("Estimated total time: " + (estimatedCost * 5.0f + 6 + 161178.0f / (80645.0 / 1.1) * dataCount * 5) + "ms");
      System.out.println("Real query time: " + lastTime + " ms");
    }
  }
}
