package example012;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.util.Iterator;

public class EvictorFlink {

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
            }).assignTimestampsAndWatermarks(new MyPeriodicAssigner()).setParallelism(1)
            .keyBy(value -> value.getSex())
            .timeWindow(Time.minutes(5))
            .evictor(new MyEvictor())
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

        public Student(String id, String name, char sex, Integer age, double height, long arriveTime) {
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

    static class MyPeriodicAssigner implements AssignerWithPeriodicWatermarks<Student> {

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

    static class MyEvictor implements Evictor<Student, TimeWindow>{

        @Override
        public void evictBefore(Iterable<TimestampedValue<Student>> elements, int size, TimeWindow window, EvictorContext evictorContext) {

        }

        @Override
        public void evictAfter(Iterable<TimestampedValue<Student>> elements, int size, TimeWindow window, EvictorContext evictorContext) {
            Iterator<TimestampedValue<Student>> it = elements.iterator();
            while (it.hasNext()){
                TimestampedValue<Student> studentTimestampedValue = it.next();
                if(studentTimestampedValue.getValue().getHeight() < 170){
                    System.err.println("removed:"+studentTimestampedValue.getValue().getHeight());
                    it.remove();
                }
            }
        }
    }

    static class MyProcessWindowFunction extends ProcessWindowFunction<Student, Double, Character,TimeWindow> {

        @Override
        public void process(Character s, Context context, Iterable<Student> elements, Collector<Double> out) throws Exception {

            double height = 0d;
            double count = 0d;
            Iterator<Student> it = elements.iterator();
            while (it.hasNext()){
                Student student = it.next();
                height += student.getHeight();
                count++;
            }
            double avgHeight = height / count ;
            out.collect(avgHeight);
        }
    }

}
