package example008;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;

/**
 * 体育老师需要按照学生报到的时间每5分钟统计一次5分钟内报到的学生的平均身高
 * 例如0~5分钟内报到的学生有A、B，0~5分钟内报到的学生的平均身高就是(A+B)/2，5~10分钟内报到的学生有C、D、E，5~10分钟内报到的学生(C+D+E)/3
 */
public class AggregateFlink {

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
                .assignTimestampsAndWatermarks(new MyPeriodicAssigner()).setParallelism(1)
                .keyBy(value -> value.getSex())
                .timeWindow(Time.minutes(5))
                .aggregate(new MyAggregateFunction())
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

    static class MyAggregateFunction implements AggregateFunction<Student, Tuple2<Double, Integer>, Double>{

        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2<>(0d,0);
        }

        @Override
        public Tuple2<Double, Integer> add(Student value, Tuple2<Double, Integer> accumulator) {
            return new Tuple2<>(accumulator.f0 + value.getHeight(),++accumulator.f1);
        }

        @Override
        public Double getResult(Tuple2<Double, Integer> accumulator) {
            return accumulator.f0.doubleValue() / accumulator.f1.doubleValue();
        }

        @Override
        public Tuple2<Double, Integer> merge(Tuple2<Double, Integer> a, Tuple2<Double, Integer> b) {
            return new Tuple2<>(a.f0 + b.f0,a.f1 + b.f1);
        }
    }

}
