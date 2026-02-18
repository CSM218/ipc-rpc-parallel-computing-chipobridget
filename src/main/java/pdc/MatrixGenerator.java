package pdc;

import java.util.Random;

/**
 * Utility class for generating and manipulating matrices.
 * Provides helper methods for creating test and example matrices.
 */
public class MatrixGenerator {

    private static final Random random = new Random();

    /**
     * Generates a random matrix of specified dimensions.
     * 
     * @param rows     number of rows
     * @param cols     number of columns
     * @param maxValue maximum value for matrix elements (exclusive)
     * @return a randomly generated matrix
     */
    public static int[][] generateRandomMatrix(int rows, int cols, int maxValue) {
        int[][] matrix = new int[rows][cols];
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                matrix[i][j] = random.nextInt(maxValue);
            }
        }
        return matrix;
    }

    /**
     * Generates an identity matrix of specified size.
     * 
     * @param size the dimension of the identity matrix
     * @return an identity matrix
     */
    public static int[][] generateIdentityMatrix(int size) {
        int[][] matrix = new int[size][size];
        for (int i = 0; i < size; i++) {
            matrix[i][i] = 1;
        }
        return matrix;
    }

    /**
     * Generates a matrix filled with a specific value.
     * 
     * @param rows  number of rows
     * @param cols  number of columns
     * @param value the value to fill the matrix with
     * @return a matrix filled with the specified value
     */
    public static int[][] generateFilledMatrix(int rows, int cols, int value) {
        int[][] matrix = new int[rows][cols];
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                matrix[i][j] = value;
            }
        }
        return matrix;
    }

    /**
     * Prints a matrix to console in a readable format.
     * 
     * @param matrix the matrix to print
     * @param label  optional label for the matrix
     */
    public static void printMatrix(int[][] matrix, String label) {
        if (label != null && !label.isEmpty()) {
            System.out.println(label);
        }
        for (int[] row : matrix) {
            for (int val : row) {
                System.out.printf("%6d ", val);
            }
            System.out.println();
        }
    }

    /**
     * Prints a matrix to console without a label.
     * 
     * @param matrix the matrix to print
     */
    public static void printMatrix(int[][] matrix) {
        printMatrix(matrix, "");
    }
}
