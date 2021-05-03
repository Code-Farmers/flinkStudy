package example006;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * 班主任制定了一些规则，规则的内容是，下课后每个学生记住自己走出教室的时间戳，之后课间休息时间结束后，学生再依次进入教室时要报出自己出教室的时间戳。班主任的记事本上也会记录一个时间戳，
 * 这个时间戳的初始值为Long.MinValue，班主任给这个时间戳起了个名字叫“水位线”，并且由于班主任允许学生最多可以，当学生报出的时间戳减去5s高于当前“水位线”时，班主任就会将学生报出的时间戳减去5s更新为新的“水位线”。
 * 同时班主任会将时间戳按5s作为一个间隔来划分，班主任称这个间隔为“窗口”，班主任会将时间戳在同一个“窗口”的同学分到一组。如果“水位线”的值高于“窗口”的最大值边界值时，则班主任会认为“窗口”内的学生都到齐了，
 * 即使后面又迟来了属于这个“窗口”的学生，
 * 也不再归纳到这个“窗口”中了。
 */
public class WatermarkFlink {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.socketTextStream("localhost",9000,"\n")
           .map(value -> {
               String[] array = value.split(",");
               String id = array[0];
               String name = array[1];
               char sex = array[2].charAt(0);
               Integer age = Integer.parseInt(array[3]);
               Long outTime = Long.parseLong(array[4]);
               Student student = new Student(id,name,sex,age,outTime);
               return student;
           }).returns(Types.GENERIC(Student.class))
                //将并行度设置为1，使得水位线统一
           .assignTimestampsAndWatermarks(new MyPunctuatedAssigner(5l)).setParallelism(1)
            .timeWindowAll(Time.milliseconds(5))
            .process(new MyProcessAllWindowFunction())
            .print();

        env.execute();
    }

    static class Student{
        private String id;

        private String name;

        private char sex;

        private Integer age;

        private long outTime;

        public Student(String id, String name, char sex, Integer age, Long outTime) {
            this.id = id;
            this.name = name;
            this.sex = sex;
            this.age = age;
            this.outTime = outTime;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public char getSex() {
            return sex;
        }

        public void setSex(char sex) {
            this.sex = sex;
        }

        public Integer getAge() {
            return age;
        }

        public void setAge(Integer age) {
            this.age = age;
        }

        public long getOutTime() {
            return outTime;
        }

        public void setOutTime(long outTime) {
            this.outTime = outTime;
        }

        @Override
        public String toString() {
            return "Student{" +
                    "id='" + id + '\'' +
                    ", name='" + name + '\'' +
                    ", sex=" + sex +
                    ", age=" + age +
                    ", outTime=" + outTime +
                    '}';
        }
    }

    static class MyPunctuatedAssigner implements AssignerWithPunctuatedWatermarks<Student>{

        private long currentMaxTimestamps = Long.MIN_VALUE;

        private Long bound;

        public MyPunctuatedAssigner(Long bound) {
            this.bound = bound;
        }

        @Nullable
        @Override
        public Watermark checkAndGetNextWatermark(Student lastElement, long extractedTimestamp) {
            currentMaxTimestamps = Math.max(currentMaxTimestamps,extractedTimestamp);
            return new Watermark(currentMaxTimestamps - bound);
        }

        @Override
        public long extractTimestamp(Student element, long previousElementTimestamp) {
            return element.getOutTime();
        }
    }

    static class MyProcessAllWindowFunction extends ProcessAllWindowFunction<Student, List, TimeWindow>{

        @Override
        public void process(Context context, Iterable<Student> elements, Collector<List> out) throws Exception {
            Iterator<Student> it = elements.iterator();
            List<Student> list = new ArrayList<>();
            while (it.hasNext()){
                list.add(it.next());
            }
            out.collect(list);
        }
    }

}
