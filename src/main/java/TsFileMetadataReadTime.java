import org.apache.iotdb.tsfile.read.TsFileSequenceReader;

public class TsFileMetadataReadTime {
  static String filePath = "/data/iotdb/sequence/root.test/0/1612026727214-1-0.tsfile";
  public static void main(String[] args) throws Exception{
    try(TsFileSequenceReader reader = new TsFileSequenceReader(filePath)) {
      long startTime = System.currentTimeMillis();

    }
  }
}
