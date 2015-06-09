package org.apache.hadoop.hdfs.server.namenode.dummy.partition;

import java.util.ArrayList;

public class KnapsackProblem {

  private Knapsack[] bags;

  private int totalWeight;

  private int n;

  private int[][] bestValues;

  private int bestValue;

  private ArrayList<Knapsack> bestSolution;

  public KnapsackProblem(Knapsack[] bags, int totalWeight) {
    this.bags = bags;
    this.totalWeight = totalWeight;
    this.n = bags.length;
    if (bestValues == null) {
      bestValues = new int[n + 1][totalWeight + 1];
    }
  }

  public void solve() {

    System.out.println("Defined External Namespace Tree：" + this.bags.length);
    for (Knapsack b : bags) {
      System.out.println(b);
    }
    System.out.println("Maximum size: " + totalWeight);

    // best value
    for (int j = 0; j <= totalWeight; j++) {
      for (int i = 0; i <= n; i++) {

        if (i == 0 || j == 0) {
          bestValues[i][j] = 0;
        } else {
          if (j < bags[i - 1].getWeight()) {
            bestValues[i][j] = bestValues[i - 1][j];
          } else {
            int iweight = bags[i - 1].getWeight();
            int ivalue = bags[i - 1].getValue();
            bestValues[i][j] =
                Math.max(bestValues[i - 1][j], ivalue
                    + bestValues[i - 1][j - iweight]);
          } // else
        } //else		 
      } //for
    } //for

    if (bestSolution == null) {
      bestSolution = new ArrayList<Knapsack>();
    }
    int tempWeight = totalWeight;
    for (int i = n; i >= 1; i--) {
      if (bestValues[i][tempWeight] > bestValues[i - 1][tempWeight]) {
        bestSolution.add(bags[i - 1]);
        tempWeight -= bags[i - 1].getWeight();
      }
      if (tempWeight == 0) {
        break;
      }
    }
    bestValue = bestValues[n][totalWeight];
  }

  public int getBestValue() {
    return bestValue;
  }

  public int[][] getBestValues() {

    return bestValues;
  }

  public ArrayList<Knapsack> getBestSolution() {
    return bestSolution;
  }

}
