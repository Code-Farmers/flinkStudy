package example009;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * 体育老师需要按照学生报到的时间每5分钟统计一次5分钟内报到的学生中身高最高的学生和身高最矮的学生，男生和女生分开统计
 */
public class ProcessFunctionFlink {

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
                    double height = Double.parseDouble(array[4]);
                    long arriveTime = Long.parseLong(array[5]);
                    Student student = new Student(id,name,sex,age,height,arriveTime);
                    return student;
                }).returns(Types.GENERIC(Student.class))
                //统一水位线
                .assignTimestampsAndWatermarks(new MyPeriodicAssigner()).setParallelism(1)
                .keyBy(value -> value.getSex())
                .timeWindow(Time.minutes(5))
                .process(new MyProcessWindowFunction())
                .print();

        env.execute();

    }

    static class Student{

        private String id;

        private String name;

        private char sex;

        private Integer age;

        private double height;

        private Long arriveTime;

        public Student(String id, String name, char sex, Integer age, double height, Long arriveTime) {
            this.id = id;
            this.name = name;
            this.sex = sex;
            this.age = age;
            this.height = height;
            this.arriveTime = arriveTime;
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

        public double getHeight() {
            return height;
        }

        public void setHeight(double height) {
            this.height = height;
        }

        public Long getArriveTime() {
            return arriveTime;
        }

        public void setArriveTime(Long arriveTime) {
            this.arriveTime = arriveTime;
        }

        @Override
        public String toString() {
            return "Student{" +
                    "id='" + id + '\'' +
                    ", name='" + name + '\'' +
                    ", sex=" + sex +
                    ", age=" + age +
                    ", height=" + height +
                    ", arriveTime=" + arriveTime +
                    '}';
        }
    }

    static class MyPeriodicAssigner implements AssignerWithPeriodicWatermarks<Student>{

        private Long currentMaxTimestamps = Long.MIN_VALUE;

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentMaxTimestamps);
        }

        @Override
        public long extractTimestamp(Student element, long previousElementTimestamp) {
            long eventTime = element.getArriveTime();
            currentMaxTimestamps = Math.max(currentMaxTimestamps,eventTime);
            return eventTime;
        }
    }

    static class MyProcessWindowFunction extends ProcessWindowFunction<Student, Map<String, Object>, Character,TimeWindow>{

        @Override
        public void process(Character s, Context context, Iterable<Student> elements, Collector<Map<String, Object>> out) throws Exception {
            Iterator<Student> it = elements.iterator();
            Student highestStudent = it.next();
            Student shortestStudent = highestStudent;
            while (it.hasNext()){
                Student student = it.next();
                if(student.getHeight() > highestStudent.getHeight()){
                    highestStudent = student;
                }

                if(student.getHeight() < shortestStudent.getHeight()){
                    shortestStudent = student;
                }
            }

            Map<String, Object> resultMap = new HashMap<>();
            resultMap.put("highestStudent",highestStudent);
            resultMap.put("shortestStudent",shortestStudent);
            resultMap.put("allStudentsInThisWindow",elements);
            out.collect(resultMap);
        }
    }
}
