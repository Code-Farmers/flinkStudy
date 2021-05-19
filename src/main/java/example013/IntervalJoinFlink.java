package example013;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.text.MessageFormat;

/**
 * 体育老师让每个男生找出到达时间不早于自己1分钟内，不晚于自己3分钟内的所有女生中身高比自己高的女生
 */
public class IntervalJoinFlink {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        KeyedStream<Student, String> maleStudentStream = env.socketTextStream("localhost",9000,"\n")
                                                    .map(value -> {
                                                        String[] array = value.split(",");
                                                        String grade = array[0];
                                                        String id = array[1];
                                                        String name = array[2];
                                                        char sex = array[3].charAt(0);
                                                        Integer age = Integer.parseInt(array[4]);
                                                        double height = Double.parseDouble(array[5]);
                                                        long arriveTime = Long.parseLong(array[6]);
                                                        IntervalJoinFlink.Student student = new IntervalJoinFlink.Student(id,name,sex,age,height,arriveTime,grade);
                                                        return student;
                                                    }).returns(Types.GENERIC(Student.class))
                                                    .assignTimestampsAndWatermarks(new MyAssignerWatermark()).setParallelism(1)
                                                    .keyBy(value -> value.getGrade());

        KeyedStream<Student, String> femaleStudentStream = env.socketTextStream("localhost",9001,"\n")
                                                    .map(value -> {
                                                        String[] array = value.split(",");
                                                        String grade = array[0];
                                                        String id = array[1];
                                                        String name = array[2];
                                                        char sex = array[3].charAt(0);
                                                        Integer age = Integer.parseInt(array[4]);
                                                        double height = Double.parseDouble(array[5]);
                                                        long arriveTime = Long.parseLong(array[6]);
                                                        IntervalJoinFlink.Student student = new IntervalJoinFlink.Student(id,name,sex,age,height,arriveTime,grade);
                                                        return student;
                                                    }).returns(Types.GENERIC(Student.class))
                                                    .assignTimestampsAndWatermarks(new MyAssignerWatermark()).setParallelism(1)
                                                    .keyBy(value -> value.getGrade());

        //男生找出到达时间不早于自己1分钟内，不晚于自己3分钟内的所有女生中身高比自己高的女生
        maleStudentStream.intervalJoin(femaleStudentStream).between(Time.minutes(-1),Time.minutes(3)).process(new MyProcessJoinFunction()).print();

        env.execute();

    }

    static class Student{

        private String grade;

        private String id;

        private String name;

        private char sex;

        private Integer age;

        private double height;

        private Long arriveTime;

        public Student(String id, String name, char sex, Integer age, double height, long arriveTime, String grade) {
            this.id = id;
            this.name = name;
            this.sex = sex;
            this.age = age;
            this.height = height;
            this.arriveTime = arriveTime;
            this.grade = grade;
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

        public String getGrade() {
            return grade;
        }

        public void setGrade(String grade) {
            this.grade = grade;
        }

        @Override
        public String toString() {
            return "Student{" +
                    "grade='" + grade + '\'' +
                    ", id='" + id + '\'' +
                    ", name='" + name + '\'' +
                    ", sex=" + sex +
                    ", age=" + age +
                    ", height=" + height +
                    ", arriveTime=" + arriveTime +
                    '}';
        }
    }

    static class MyAssignerWatermark implements AssignerWithPeriodicWatermarks<Student>{

        private long currentMaxTimestamps = Long.MIN_VALUE;

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentMaxTimestamps);
        }

        @Override
        public long extractTimestamp(Student element, long previousElementTimestamp) {
            long eventTime = element.getArriveTime();
            currentMaxTimestamps = Math.max(eventTime,currentMaxTimestamps);
            return eventTime;
        }
    }

    static class MyProcessJoinFunction extends ProcessJoinFunction<Student,Student, String>{

        @Override
        public void processElement(Student left, Student right, Context ctx, Collector<String> out) throws Exception {
            if(left.getHeight() < right.getHeight()){
                out.collect(MessageFormat.format("male student[id:{0},name:{1},height:{2},arriveTime:{3}] is short than female student[id:{4},name:{5},height:{6},arriveTime:{7}]",
                        left.getId(),left.getName(),left.getHeight(),left.getArriveTime(),right.getId(),right.getName(),right.getHeight(),right.getArriveTime()));
            }
        }
    }

}
