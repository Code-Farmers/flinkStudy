package example007;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 班主任需要实时统计出报到的所有学生中年龄最大的学生，年龄最大的学生有多个时取最先报到的学生就行
 */
public class ReduceFlink {

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
                Student student = new Student(id,name,sex,age);
                return student;
            }).returns(Types.GENERIC(Student.class))
           .keyBy(value -> value.getSex())
           .reduce(new MyReduceFunction()).setParallelism(3)
           .print();

        env.execute();
    }

    static class Student{
        private String id;

        private String name;

        private char sex;

        private Integer age;

        public Student(String id, String name, char sex, Integer age) {
            this.id = id;
            this.name = name;
            this.sex = sex;
            this.age = age;
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

        @Override
        public String toString() {
            return "Student{" +
                    "id='" + id + '\'' +
                    ", name='" + name + '\'' +
                    ", sex=" + sex +
                    ", age=" + age +
                    '}';
        }
    }

    static class MyReduceFunction implements ReduceFunction<Student>{

        @Override
        public Student reduce(Student value1, Student value2) throws Exception {
            return value2.getAge() > value1.getAge() ? value2 : value1;
        }
    }

}
