package com.wyd.userdraw;

import java.math.BigDecimal;

public class UserDraw {
    private String startTimeDay;

    private String MDN;

    private Double male;

    private Double female;

    private Double age1;

    private Double age2;

    private Double age3;

    private Double age4;

    private Double age5;

    //性别融合
    public void protraitSex(Double male2, Double female2, Long times){
        Double sum = (this.male + this.female + (male2 + female2) * times);
        if(sum != 0){
            this.male = (this.male + male2 * times) / sum;
            this.female = (this.female + female2 * times) /sum;
        }
    }

    //年龄段融合
    public void protraitAge(Double nAge1, Double nAge2, Double nAge3, Double nAge4, Double nAge5, Long times){
        Double sum = (age1 + age2 + age3 + age4 + age5 + (nAge1 + nAge2 + nAge3 + nAge4 + nAge5) * times);
        if(sum != 0){
            age1 = (age1 + nAge1 * times) / sum;
            age2 = (age2 + nAge2 * times) / sum;
            age3 = (age3 + nAge3 * times) / sum;
            age4 = (age4 + nAge4 * times) / sum;
            age5 = (age5 + nAge5 * times) / sum;
        }
    }

    //初始化性别概率
    public void initSex(Double male, Double female){
        Double sum = male + female;
        if(sum != 0){
            this.male = male / sum;
            this.female = female / sum;
        }
    }

    //初始化年龄段概率
    public void initAge(Double age1, Double age2, Double age3, Double age4, Double age5){
        Double sum = age1 + age2 + age3 + age4 + age5;
        if(sum != 0){
            this.age1 = age1 / sum;
            this.age2 = age2 / sum;
            this.age3 = age3 / sum;
            this.age4 = age4 / sum;
            this.age5 = age5 / sum;
        }
    }

    //重写toString方法
    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append(startTimeDay).append("|");
        sb.append(MDN).append("|");
        sb.append(new BigDecimal(male).setScale(3, BigDecimal.ROUND_HALF_UP).doubleValue()).append("|");
        sb.append(new BigDecimal(female).setScale(3, BigDecimal.ROUND_HALF_UP).doubleValue()).append("|");
        sb.append(new BigDecimal(age1).setScale(3, BigDecimal.ROUND_HALF_UP).doubleValue()).append("|");
        sb.append(new BigDecimal(age2).setScale(3, BigDecimal.ROUND_HALF_UP).doubleValue()).append("|");
        sb.append(new BigDecimal(age3).setScale(3, BigDecimal.ROUND_HALF_UP).doubleValue()).append("|");
        sb.append(new BigDecimal(age4).setScale(3, BigDecimal.ROUND_HALF_UP).doubleValue()).append("|");
        sb.append(new BigDecimal(age5).setScale(3, BigDecimal.ROUND_HALF_UP).doubleValue()).append("|");
        return sb.toString();
    }

    public String getStartTimeDay() {
        return startTimeDay;
    }

    public void setStartTimeDay(String startTimeDay) {
        this.startTimeDay = startTimeDay;
    }

    public String getMDN() {
        return MDN;
    }

    public void setMDN(String MDN) {
        this.MDN = MDN;
    }

    public Double getMale() {
        return male;
    }

    public void setMale(Double male) {
        this.male = male;
    }

    public Double getFemale() {
        return female;
    }

    public void setFemale(Double female) {
        this.female = female;
    }

    public Double getAge1() {
        return age1;
    }

    public void setAge1(Double age1) {
        this.age1 = age1;
    }

    public Double getAge2() {
        return age2;
    }

    public void setAge2(Double age2) {
        this.age2 = age2;
    }

    public Double getAge3() {
        return age3;
    }

    public void setAge3(Double age3) {
        this.age3 = age3;
    }

    public Double getAge4() {
        return age4;
    }

    public void setAge4(Double age4) {
        this.age4 = age4;
    }

    public Double getAge5() {
        return age5;
    }

    public void setAge5(Double age5) {
        this.age5 = age5;
    }
}
