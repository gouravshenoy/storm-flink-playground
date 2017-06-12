package edu.iu.mathop;

/**
 * Created by goshenoy on 6/11/17.
 */
public class Data {

    private Integer number1;
    private Integer number2;

    public Data(Integer number1, Integer number2) {
        this.number1 = number1;
        this.number2 = number2;
    }

    public Integer getNumber1() {
        return number1;
    }

    public void setNumber1(Integer number1) {
        this.number1 = number1;
    }

    public Integer getNumber2() {
        return number2;
    }

    public void setNumber2(Integer number2) {
        this.number2 = number2;
    }

    @Override
    public String toString() {
        return "Data{" +
                "number1=" + number1 +
                ", number2=" + number2 +
                '}';
    }
}
