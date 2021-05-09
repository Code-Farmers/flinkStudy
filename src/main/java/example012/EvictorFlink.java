package example012;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.util.Iterator;

/**
 * 体育老师要求按照学生到达操场的时间以[0,10)、[10,20)...这样每10分钟划分为一个时间窗口，每个窗口内每到三个学生就统计一次该窗口内的所有学生的平均身高，之后将低于
 * 平均身高的学生从窗口中剔除出去，每次统计的平均身高需要输出并打印，男女分开统计（此例为了方便观察结果便将测试数据都设置为了男性）。
 * 2021-04-19 21:24:50 tom到达 身高168
 * 2021-04-19 21:25:50 lucy到达 身高162
 * 2021-04-19 21:26:50 kate到达 身高173
 * 此时由于[0,10)时间窗口内来了三个学生，触发了该窗口的计算，于是对tom、lucy、kate三人取平均身高为167.6666666666667
 * 由于lucy的身高低于平均身高，所以被剔除了，此时窗口内只剩下tom和kate
 * 接着
 * 2021-04-19 21:27:50 bob到达 身高186
 * 2021-04-19 21:28:50 peter到达 身高178
 * 2021-04-19 21:29:50 jack到达 身高171
 * 此时由于[0,10)时间窗口内又来了以上三人，再次触发了窗口的计算，于是对tom、kate、bob、peter、jack五人取平均身高为175.2
 * 由于tom、kate、jack的身高都低于平均身高，所以被剔除了
 * 以此类推....
 */
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
                    EvictorFlink.Student student = new EvictorFlink.Student(id,name,sex,age,height,arriveTime);
                    return student;
                }).returns(Types.GENERIC(EvictorFlink.Student.class))
                .assignTimestampsAndWatermarks(new EvictorFlink.MyPeriodicAssigner()).setParallelism(1)
                .keyBy(value -> value.getSex())
                .timeWindow(Time.minutes(10))
                .trigger(new EvictorFlink.MyTrigger())
                .evictor(new EvictorFlink.MyEvictor())
                .process(new EvictorFlink.MyProcessWindowFunction())
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
        public long extractTimestamp(EvictorFlink.Student element, long previousElementTimestamp) {
            long eventTime = element.getArriveTime();
            currentMaxTimestamps = Math.max(currentMaxTimestamps,eventTime);
            return eventTime;
        }
    }

    static class MyTrigger extends Trigger<Student, TimeWindow> {

        private ValueState<Integer> preCountValue;

        @Override
        public TriggerResult onElement(EvictorFlink.Student element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            ValueStateDescriptor<Integer> preCountDesc = new ValueStateDescriptor<Integer>("preCount",Types.INT);
            preCountValue = ctx.getPartitionedState(preCountDesc);

            if(preCountValue.value() == null){
                preCountValue.update(0);
            }

            ctx.registerEventTimeTimer(window.getEnd());

            //每当来了3个元素或者来了窗口内的最后一个元素时触发窗口的计算
            preCountValue.update(preCountValue.value() + 1);
            if(preCountValue.value() >= 3){
                preCountValue.update(0);
                return TriggerResult.FIRE;
            }else{
                return TriggerResult.CONTINUE;
            }
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.FIRE;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
            ctx.deleteEventTimeTimer(window.getEnd());
        }
    }

    static class MyEvictor implements Evictor<Student,TimeWindow>{

        private ValueState<Double> avgValueState;

        @Override
        public void evictBefore(Iterable<TimestampedValue<Student>> elements, int size, TimeWindow window, EvictorContext evictorContext) {

        }

        @Override
        public void evictAfter(Iterable<TimestampedValue<Student>> elements, int size, TimeWindow window, EvictorContext evictorContext) {

            Iterator<TimestampedValue<Student>> it1 = elements.iterator();
            double totalHeight = 0;
            double count = 0;
            while (it1.hasNext()){
                TimestampedValue<Student> student = it1.next();
                totalHeight += student.getValue().getHeight();
                count++;
            }

            double avgHeight = totalHeight / count;
            Iterator<TimestampedValue<Student>> it2 = elements.iterator();
            while (it2.hasNext()){
                if(it2.next().getValue().getHeight() < avgHeight){
                    it2.remove();
                }
            }
        }
    }

    static class MyProcessWindowFunction extends ProcessWindowFunction<Student, Double, Character,TimeWindow>{

        private ValueState<Double> avgValueState;

        @Override
        public void process(Character character, Context context, Iterable<Student> elements, Collector<Double> out) throws Exception {
            ValueStateDescriptor<Double> avgValueStateDesc = new ValueStateDescriptor<Double>("avgValueStateDesc",Types.DOUBLE);
            avgValueState = context.windowState().getState(avgValueStateDesc);
            Iterator<Student> it = elements.iterator();
            double totalHeight = 0;
            double count = 0;
            while (it.hasNext()){
                EvictorFlink.Student student = it.next();
                totalHeight += student.getHeight();
                count++;
            }

            double avgHeight = totalHeight / count;
            avgValueState.update(avgHeight);
            out.collect(avgHeight);
        }
    }

}
