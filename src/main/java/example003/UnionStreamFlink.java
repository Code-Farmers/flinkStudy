package example003;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 使用union将两个流进行合并
 * example003_male.txt中有男学生的信息
 * example003_female.txt中有女学生的信息
 * 现需要将男、女同学的信息汇总后一并输出
 */
public class UnionStreamFlink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Student> maleDataStream = env.readTextFile(UnionStreamFlink.class.getResource("/example003_male.txt").toString())
                                                .map(value -> {
                                                    String[] array = value.split(",");
                                                    String id = array[0];
                                                    String name = array[1];
                                                    char sex = array[2].charAt(0);
                                                    Integer age = Integer.parseInt(array[3]);
                                                    Student student = new Student(id,name,sex,age);
                                                    return student;
                                                }).returns(Types.GENERIC(Student.class));

        DataStream<Student> femaleDataStream = env.readTextFile(UnionStreamFlink.class.getResource("/example003_female.txt").toString())
                .map(value -> {
                    String[] array = value.split(",");
                    String id = array[0];
                    String name = array[1];
                    char sex = array[2].charAt(0);
                    Integer age = Integer.parseInt(array[3]);
                    Student student = new Student(id,name,sex,age);
                    return student;
                }).returns(Types.GENERIC(Student.class));

        //使用union进行双流合并，注意union只能对元素类型相同的流进行合并
        maleDataStream.union(femaleDataStream).print();

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
