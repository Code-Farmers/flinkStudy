package example004;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * 使用connect将两条流进行合并
 * 第一条流中的元素是每个考生的信息，包含姓名、年龄、成绩
 * 第二条流中的元素是最新的录取分数线
 */
public class ConnectStreamFlink {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //因为MapFunction中是用的成员变量来存储最新的录取分数线，而不是用flink的状态后端来进行存储的，所以必须限制并行度为1才能使本例成立
        env.setParallelism(1);
        //为了随时可以调整考生信息以及录取分数线，所以此处使用socket来作为数据源
        DataStream<Student> studentStream = env.socketTextStream("localhost",9000,"\n")
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

        DataStream<Double> passingScoreStream = env.socketTextStream("localhost",9001,"\n")
                                                .map(value -> Double.parseDouble(value)).returns(Types.DOUBLE);

        studentStream.connect(passingScoreStream).map(new AdmitCoMapFunction(500)).print();

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

    static class AdmitCoMapFunction implements CoMapFunction<Student, Double, Tuple3<Student, Double, String>>{

        //为了简化，此处用的是成员变量，而不是状态后端
        private double currentPassingScore;

        public AdmitCoMapFunction(double currentPassingScore) {
            this.currentPassingScore = currentPassingScore;
        }

        @Override
        public Tuple3<Student, Double, String> map1(Student value) throws Exception {
            if(value.getScore() >= currentPassingScore){
                return new Tuple3<>(value,currentPassingScore,"pass");
            }else{
                return new Tuple3<>(value,currentPassingScore,"not pass");
            }
        }

        @Override
        public Tuple3<Student, Double, String> map2(Double value) throws Exception {
            this.currentPassingScore = value;
            return new Tuple3<>(null,currentPassingScore,"change passing score");
        }
    }

}
