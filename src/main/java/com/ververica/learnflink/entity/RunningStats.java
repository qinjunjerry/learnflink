package com.ververica.learnflink.entity;

import java.text.DecimalFormat;

public class RunningStats {
    private static DecimalFormat formatter = new DecimalFormat("##.##");

    private long num;
    private double min;
    private double max;
    private double total;

    public RunningStats() {
        num = 0;
        min = Double.MAX_VALUE;
        max = Double.MIN_VALUE;
        total = 0;
    }

    public void add(double amount) {
        num++;
        min = Math.min(amount, min);
        max = Math.max(amount, max);
        total += amount;
    }

    public double average() {
        return Double.parseDouble(
                formatter.format(
                        Math.round(100.0 * total / num) / 100.0
                )
        );
    }

    public long getNum() {
        return num;
    }

    public double getMin() {
        return min;
    }

    public double getMax() {
        return max;
    }

    public double getTotal() {
        return Double.parseDouble(
                formatter.format(
                        Math.round(100.0 * total) / 100.0
                )
        );
    }
}

