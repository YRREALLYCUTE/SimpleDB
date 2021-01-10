package simpledb;

import java.util.Arrays;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int buckets;
    private int max;
    private int min;

    private double width;
    private int[] height;
    /**
     * Create a new IntHistogram.
     *
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     *
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     *
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        // some code goes here
        this.buckets = buckets;
        this.min = min;
        this.max = max;

        // width 指的是每个bucket的宽度
        this.width = (double)(max - min) / buckets;
        // height是一个数组，height[i]存储第i个bucket中的元组数量
        this.height = new int[buckets];
        for(int i = 0;i < this.buckets;i++){
            this.height[i] = 0;
        }
    }

    private int getIndex(int v){
        return v == max ? this.buckets -1 : (int) Math.floor((v- min) / width);
    }
    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        // some code goes here
        int index = getIndex(v);
        height[index] += 1;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     *
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

        // some code goes here
        double estimate;
        switch (op){

            case EQUALS:
                estimate = equal(v);
                break;
            case GREATER_THAN:
                estimate = greaterThan(v);
                break;
            case LESS_THAN:
                estimate = lessThan(v);
                break;
            case LESS_THAN_OR_EQ:
                estimate = lessThan(v) + equal(v);
                if(estimate > 1) estimate = 1;
                if(estimate < 0) estimate = 0;
                break;
            case GREATER_THAN_OR_EQ:
                estimate = greaterThan(v) + equal(v);
                if(estimate > 1) estimate = 1;
                if(estimate < 0) estimate = 0;
                break;
            case NOT_EQUALS:
                estimate = 1 - equal(v);
                break;
            default:
                estimate = 0.0;
        }

        return estimate;
    }


    private double greaterThan(int v){
        double res = 0.0;
        int i = getIndex(v);

        if(v <= min)
            res = 1.0;
        else if(v >= max)
            res = 0.0;
        else {
            double right = i * width + width + min;
            // 2 第二种猜想：使用bucket中的整数个数作为除数
            double left = i * width + min;
            int k = (int)(Math.floor(right) - Math.ceil(left) + 1);

            int ntups = Arrays.stream(height).sum();

            for (int j = i + 1; j < buckets; j++) {
                res += (double) height[j] / ntups;
            }

            double b_f = (double) height[i] / ntups;
            // double b_part = (right - v) / width;
            // 2
            double b_part = k == 0 ? 0 : (right - v) / (double)k;
            res = b_f * b_part  + res;
        }
        return res;
    }

    private double lessThan(int v){
        double res = 0.0;
        int i = getIndex(v);

        if(v <= min)
            res = 0.0;
        else if(v >= max)
            res = 1.0;
        else {
            double left = i * width + min;
            // 第二种猜想
            double right = i * width + min + width;
            int k = (int) (Math.floor(right) - Math.ceil(left) + 1);

            int ntups = Arrays.stream(height).sum();

            for (int j = 0; j < i ; j++) {
                res += (double) height[j] / ntups;
            }

            double b_f = (double) height[i] / ntups;
            // double b_part = (v - left) / width;
            double b_part = k == 0 ? 0 : (v - left) / (double)k;

            res = b_f * b_part  + res;
        }
        return res;
    }

    private double equal(int v){
        double res = 0.0;

        if(v > max || v < min){
            return res;
        }
        else{
            int i = getIndex(v);

            // 第二种猜想
            double left = i * width + min;
            double right = i * width + min + width;
            int k = (int) (Math.floor(right) - Math.ceil(left) + 1);

            int ntups = Arrays.stream(height).sum();
            // if(width < 1) res = (double)height[i] / ntups;
            // else
            //    res = height[i] / width / ntups;
            res = k == 0 ? 0 : (double)height[i] / k / ntups;
        }


        return res;
    }
    /**
     * @return
     *     the average selectivity of this histogram.
     *
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        int cnt = 0;
        for(int bucket: height) cnt += bucket;
        int ntups = Arrays.stream(height).sum();
        if(cnt ==0) return 0.0;
        return cnt/ntups;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return String.format("buckets: %d, max: %d, min: %d, width: %f", buckets, max, min, width);
    }
}
