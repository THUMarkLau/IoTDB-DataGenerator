import com.csvreader.CsvReader;
import org.apache.iotdb.tsfile.utils.Pair;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CostEstimator {
  private static List<Pair<Long, Float>> empiricalData = new ArrayList<>();
  private static boolean init = false;
  static boolean readEmpiricalFile() {
    final File EMPIRICAL_SEEK_FILE = new File("/data/iotdb/empiricalData/seek_time.csv");
    try{
      if (!EMPIRICAL_SEEK_FILE.exists()) {
        return false;
      }
      CsvReader csvReader = new CsvReader(EMPIRICAL_SEEK_FILE.getAbsolutePath());
      csvReader.readHeaders();
      while(csvReader.readRecord()) {
        String blockSize = csvReader.get("BlockSize");
        String averageTime = csvReader.get("AverageTime");
        empiricalData.add(new Pair<Long, Float>(stringDataToBytes(blockSize), Float.valueOf(averageTime)));
      }
      csvReader.close();
      return true;
    }
    catch (IOException e) {
      e.printStackTrace();
      return false;
    }
  }

  static long stringDataToBytes(String data) {
    String[] suffixes = {"B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"};
    long result = 0;
    for(int i = suffixes.length - 1; i >= 0; --i) {
      if (data.endsWith(suffixes[i])) {
        int pos = data.indexOf(suffixes[i]);
        float base = Float.valueOf(data.substring(0, pos));
        for(int j = 0; j < i; ++j) {
          base *= 1024.0;
        }
        result = (long)base;
        break;
      }
    }
    return result;
  }

  static float getSeekCost(long distance) {
    if (!init) {
      readEmpiricalFile();
      init = true;
    }
    System.out.println("Distance: "+distance);
    float seekCost = 0;
    for(int i = 0; i < empiricalData.size() - 1; ++i) {
      if (distance >= empiricalData.get(i).left && distance < empiricalData.get(i + 1).left) {
        seekCost = (float)(distance - empiricalData.get(i).left) / (float)(empiricalData.get(i+1).left - empiricalData.get(i).left);
        seekCost = seekCost * (empiricalData.get(i+1).right - empiricalData.get(i).right) + empiricalData.get(i).right;
        break;
      }
    }
    return seekCost;
  }
}
