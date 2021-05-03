package example001;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * flink读取文件
 */
public class FileSourceFlink {
    public static void main(String[] args) throws Exception {
        //创建flink运行时环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //创建数据源
        DataStream<String> dataStream = env.readTextFile(FileSourceFlink.class.getResource("/example001.txt").toString());

        //创建数据汇
        dataStream.print();

        //启动并运行flink环境
        env.execute();
    }
}
