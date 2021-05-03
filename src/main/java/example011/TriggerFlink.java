package example011;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.util.Iterator;

/**
 * 体育老师需要按照学生报到的时间每5分钟统计一次5分钟内报到的学生的平均身高,且如果5分钟内报到的学生数超过3个时，每报到一个学生就立即统计一次
 */
public class TriggerFlink {

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
                    })
                    .assignTimestampsAndWatermarks(new MyPeriodicAssigner()).setParallelism(1)
                    .keyBy(value -> value.getSex())
                    .timeWindow(Time.minutes(5))
                    .trigger(new MyTrigger())
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

    static class MyTrigger extends Trigger<Student, TimeWindow>{

        @Override
        public TriggerResult onElement(Student element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {

            ValueState<Integer> windowElementsNumState = ctx.getPartitionedState(new ValueStateDescriptor<Integer>("windowElementsNum", Types.INT));

            //如果是进入到窗口的第一个元素，则windowElementsNumState的值之前没有设置过，也就是null
            Integer windowElementsNum;
            if(windowElementsNumState.value() == null){
                ctx.registerEventTimeTimer(window.getEnd());
                windowElementsNum = 1;
                windowElementsNumState.update(windowElementsNum);
            }else{
                windowElementsNum = windowElementsNumState.value();
                windowElementsNum++;
                windowElementsNumState.update(windowElementsNum);
            }

            if(windowElementsNum > 3){
                return TriggerResult.FIRE;
            }
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.FIRE_AND_PURGE;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
            ctx.getPartitionedState(new ValueStateDescriptor<Boolean>("first", Types.BOOLEAN)).clear();
            ctx.getPartitionedState(new MapStateDescriptor<String, Double>("avgHeightValueState", Types.STRING ,Types.DOUBLE)).clear();
        }
    }

    static class MyProcessWindowFunction extends ProcessWindowFunction<Student, Double, Character,TimeWindow>{

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
