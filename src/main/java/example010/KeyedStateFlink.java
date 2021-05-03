package example010;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 体育老师要求每报到一个学生后统计一次截止目前所有报到的学生的平均身高，男生和女生分开统计
 */
public class KeyedStateFlink {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.socketTextStream("localhost",9000,"\n")
                .map(value -> {
                    String[] array = value.split(",");
                    String id = array[0];
                    String name = array[1];
                    char sex = array[2].charAt(0);
                    Integer age = Integer.parseInt(array[3]);
                    double height = Double.parseDouble(array[4]);
                    Student student = new Student(id,name,sex,age,height);
                    return student;
                })
                .keyBy(value -> value.getSex())
                .process(new MyKeyedProcessFunction())
                .print();

        env.execute();
    }

    static class Student{

        private String id;

        private String name;

        private char sex;

        private Integer age;

        private double height;

        public Student(String id, String name, char sex, Integer age, double height) {
            this.id = id;
            this.name = name;
            this.sex = sex;
            this.age = age;
            this.height = height;
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

        @Override
        public String toString() {
            return "Student{" +
                    "id='" + id + '\'' +
                    ", name='" + name + '\'' +
                    ", sex=" + sex +
                    ", age=" + age +
                    ", height=" + height +
                    '}';
        }
    }

    static class MyKeyedProcessFunction extends KeyedProcessFunction<Character,Student, Double>{

        private MapState<String, Double> avgHeightValueState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            MapStateDescriptor<String, Double> mapStateDescriptor = new MapStateDescriptor<String, Double>("avgHeightValueState", Types.STRING ,Types.DOUBLE);
            avgHeightValueState = getRuntimeContext().getMapState(mapStateDescriptor);
        }

        @Override
        public void processElement(Student value, Context ctx, Collector<Double> out) throws Exception {

             double totalHeight;
             if(avgHeightValueState.get("totalHeight") == null){
                 totalHeight = 0;
              }else{
                 totalHeight = avgHeightValueState.get("totalHeight");
             }

             double totalCount;
             if(avgHeightValueState.get("totalCount") == null){
                 totalCount = 0;
             }else{
                 totalCount = avgHeightValueState.get("totalCount");
             }

             totalHeight += value.getHeight();
             totalCount++;
             double avgHeight = totalHeight / totalCount;
             avgHeightValueState.put("totalHeight",totalHeight);
            avgHeightValueState.put("totalCount",totalCount);
            out.collect(avgHeight);
        }
    }

}
