package org.apache.hadoop.hdfs.server.namenode.dummy.partition;

public class KnapsackTest {

  public void test1() {
    int value2 = 2;
    Knapsack[] bags =
        new Knapsack[] { new Knapsack(1, 5), new Knapsack(value2, 14),
            //new Knapsack(value2,10), new Knapsack(value2,35),
            //new Knapsack(value2,25),new Knapsack(value2,2),
            new Knapsack(value2, 1), new Knapsack(1, 3), new Knapsack(1, 2) };
    int totalWeight = 6;
    KnapsackProblem kp = new KnapsackProblem(bags, totalWeight);

    kp.solve();
    System.out.println(" -------- Found the solution: --------- ");
    System.out.println("Found maximum value：" + kp.getBestValue());
    System.out.println("The best solution【Selected external namespace tree】: ");
    System.out.println(kp.getBestSolution());

    System.out.println("Matrix：");
    int[][] bestValues = kp.getBestValues();
    for (int i = 0; i < bestValues.length; i++) {
      for (int j = 0; j < bestValues[i].length; j++) {
        System.out.printf("%-5d", bestValues[i][j]);
      }
      System.out.println();
    }

  }

  public void test2() {
    int value2 = 2;
    Knapsack[] bags =
        new Knapsack[] { new Knapsack(1, 5), new Knapsack(value2, 14),
            new Knapsack(value2, 10), new Knapsack(value2, 35),
            new Knapsack(value2, 25), new Knapsack(value2, 2),
            new Knapsack(value2, 1), new Knapsack(1, 11), new Knapsack(1, 3),
            new Knapsack(1, 1), new Knapsack(1, 2), new Knapsack(value2, 3),
            new Knapsack(value2, 4) };
    int totalWeight = 10;
    KnapsackProblem kp = new KnapsackProblem(bags, totalWeight);

    kp.solve();
    System.out.println(" -------- Found the solution: --------- ");
    System.out.println("Found maximum value：" + kp.getBestValue());
    System.out.println("The best solution【Selected external namespace tree】: ");
    System.out.println(kp.getBestSolution());
  }

  public static void main(String[] args) {
    KnapsackTest kt = new KnapsackTest();
    kt.test1();
    //kt.test2();
  }
}
