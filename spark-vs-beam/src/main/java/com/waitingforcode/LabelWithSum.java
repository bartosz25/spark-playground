package com.waitingforcode;

import java.io.Serializable;

public class LabelWithSum implements Serializable {

    private String label;
    private int sum;

    public LabelWithSum(String label, int sum) {
        this.label = label;
        this.sum = sum;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public int getSum() {
        return sum;
    }

    public void setSum(int sum) {
        this.sum = sum;
    }
}
