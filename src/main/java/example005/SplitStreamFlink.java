package example005;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class SplitStreamFlink {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Student> studentDataStream = env.readTextFile(SplitStreamFlink.class.getResource("/example005.txt").toString())
                .map(value -> {
                    String[] array = value.split(",");
                    String id = array[0];
                    String name = array[1];
                    char sex = array[2].charAt(0);
                    Integer age = Integer.parseInt(array[3]);
                    Double score = Double.parseDouble(array[4]);
                    Student student = new Student(id,name,sex,age,score);
                    return student;
                }).returns(Types.GENERIC(Student.class));

        SplitStream<Student> splitStream = studentDataStream.split(value -> {
                                                                List<String> outputTags = new ArrayList<>();
                                                                if(value.getScore() > 600){
                                                                     outputTags.add("admitted");
                                                                }else{
                                                                    outputTags.add("not admitted");
                                                                }
                                                                return outputTags;
                                                            });

        DataStream<Student> admittedStudentStream = splitStream.select("admitted");
        DataStream<Student> notAdmittedStudentStream = splitStream.select("not admitted");

        admittedStudentStream.print("admitted");
        notAdmittedStudentStream.print("not admitted");

        env.execute();
    }


    static class Student{
        private String id;

        private String name;

        private char sex;

        private Integer age;

        private Double score;

        public Student(String id, String name, char sex, Integer age, Double score) {
            this.id = id;
            this.name = name;
            this.sex = sex;
            this.age = age;
            this.score = score;
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

        public Double getScore() {
            return score;
        }

        public void setScore(Double score) {
            this.score = score;
        }

        @Override
        public String toString() {
            return "Student{" +
                    "id='" + id + '\'' +
                    ", name='" + name + '\'' +
                    ", sex=" + sex +
                    ", age=" + age +
                    ", score=" + score +
                    '}';
        }
    }

}
