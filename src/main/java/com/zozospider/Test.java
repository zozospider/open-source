package com.zozospider;

public class Test {

    public static void main(String[] args) {
        System.out.println((Runtime.getRuntime().maxMemory()));
        System.out.println((long)(Runtime.getRuntime().maxMemory() * .80));
        System.out.println((long)((1908932608 * 0.80) * (1 - 20 * 0.01)) / 100);
        System.out.println((long)(1527146086 * (1 - 20 * 0.01)) / 100);
        System.out.println((int) (( (long)(Runtime.getRuntime().maxMemory() * .80) *
                (1 - 20 * .01)) / 100));
        System.out.println(Integer.MAX_VALUE);

        System.out.println((int) (((long)(Runtime.getRuntime().maxMemory() * .80) * (1 - 20 * .01)) /
                100));
    }

}
