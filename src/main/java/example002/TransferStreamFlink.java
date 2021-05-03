package example002;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 1、使用map算子将元素转换成student类型
 * 2、使用keyby将student按照sex进行分组
 */
public class TransferStreamFlink {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> dataStream = env.readTextFile(TransferStreamFlink.class.getResource("/example002.txt").toString());

        DataStream<Student> studentStream = dataStream.map(value -> {
            //1,tom,m,11
            String[] array = value.split(",");
            String id = array[0];
            String name = array[1];
            char sex = array[2].charAt(0);
            Integer age = Integer.parseInt(array[3]);
            Student student = new Student(id,name,sex,age);
            return student;
        }).returns(Types.GENERIC(Student.class));

        DataStream<Student> keyedStream = studentStream.keyBy(value -> value.getSex());

        keyedStream.print();

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

}
