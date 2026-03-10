/*
 * ===========================================================================
 *  Lab - 06: Movie Recommendation System using Apache Spark MLlib (ALS)
 * ===========================================================================
 *
 *  Course  : Big Data Analytics Lab
 *  Task    : Build a recommendation system using Spark and MLlib on Hadoop
 *  Language: Java (No Python)
 *
 *  ALGORITHM: Alternating Least Squares (ALS) - Collaborative Filtering
 *  ---------------------------------------------------------------------------
 *  ALS is a matrix factorization algorithm used for collaborative filtering.
 *  It decomposes the user-item rating matrix into two lower-rank matrices:
 *      - User factor matrix (U)
 *      - Item factor matrix (V)
 *  Such that: R ≈ U × V^T
 *
 *  The algorithm alternates between:
 *      1. Fixing V and solving for U
 *      2. Fixing U and solving for V
 *  Until convergence (minimizing the squared error with regularization).
 *
 *  DATASET FORMAT (ratings.csv on HDFS):
 *  ---------------------------------------------------------------------------
 *  userId,movieId,rating,timestamp
 *  1,1,4.0,964982703
 *  1,3,4.0,964981247
 *  ...
 *
 *  WORKFLOW:
 *  ---------------------------------------------------------------------------
 *  1. Load ratings data from HDFS into a Spark DataFrame
 *  2. Split data into training (80%) and test (20%) sets
 *  3. Build ALS model on the training set
 *  4. Evaluate model using RMSE (Root Mean Squared Error) on the test set
 *  5. Generate top-N movie recommendations for each user
 *  6. Generate top-N user recommendations for each movie
 *  7. Save recommendations back to HDFS
 *
 *  COMPILATION & EXECUTION:
 *  ---------------------------------------------------------------------------
 *  See README.md for full instructions.
 *
 * ===========================================================================
 */

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.ml.evaluation.RegressionEvaluator;

public class MovieRecommendation {

    public static void main(String[] args) {

        // =====================================================================
        // STEP 1: Validate command-line arguments
        // =====================================================================
        if (args.length < 2) {
            System.err.println("Usage: MovieRecommendation <input-path> <output-path>");
            System.err.println("  <input-path>  : HDFS path to ratings.csv");
            System.err.println("  <output-path> : HDFS path for output recommendations");
            System.exit(1);
        }

        String inputPath = args[0];    // e.g., hdfs://localhost:9000/input/ratings.csv
        String outputPath = args[1];   // e.g., hdfs://localhost:9000/output/recommendations

        // =====================================================================
        // STEP 2: Initialize SparkSession with Hadoop integration
        // =====================================================================
        // SparkSession is the unified entry point for Spark functionality.
        // Setting master("local[*]") uses all available CPU cores.
        // In a cluster, this would be set to "yarn" or "spark://master:7077".
        SparkSession spark = SparkSession.builder()
                .appName("MovieRecommendationSystem")
                .master("local[*]")
                .getOrCreate();

        // Set log level to reduce verbose output
        spark.sparkContext().setLogLevel("ERROR");

        System.out.println("============================================================");
        System.out.println("  Movie Recommendation System using Spark MLlib ALS");
        System.out.println("============================================================");
        System.out.println("Input Path  : " + inputPath);
        System.out.println("Output Path : " + outputPath);
        System.out.println();

        // =====================================================================
        // STEP 3: Define schema and load ratings data from HDFS
        // =====================================================================
        // Defining an explicit schema avoids the overhead of schema inference
        // and ensures correct data types.
        StructType ratingsSchema = new StructType(new StructField[]{
                DataTypes.createStructField("userId", DataTypes.IntegerType, false),
                DataTypes.createStructField("movieId", DataTypes.IntegerType, false),
                DataTypes.createStructField("rating", DataTypes.FloatType, false),
                DataTypes.createStructField("timestamp", DataTypes.LongType, false)
        });

        // Read CSV file from HDFS with header and the defined schema
        Dataset<Row> ratings = spark.read()
                .option("header", "true")   // First row is header
                .schema(ratingsSchema)       // Apply our schema
                .csv(inputPath);             // Read from HDFS path

        // Display dataset statistics
        long totalRatings = ratings.count();
        long totalUsers = ratings.select("userId").distinct().count();
        long totalMovies = ratings.select("movieId").distinct().count();

        System.out.println("--- Dataset Statistics ---");
        System.out.println("Total Ratings : " + totalRatings);
        System.out.println("Total Users   : " + totalUsers);
        System.out.println("Total Movies  : " + totalMovies);
        System.out.println();

        // Show first 10 rows of the dataset
        System.out.println("--- Sample Ratings Data ---");
        ratings.show(10);

        // =====================================================================
        // STEP 4: Split data into Training (80%) and Test (20%) sets
        // =====================================================================
        // The training set is used to build the model, and the test set is
        // used to evaluate how well the model generalizes to unseen data.
        Dataset<Row>[] splits = ratings.randomSplit(new double[]{0.8, 0.2}, 42L);
        Dataset<Row> trainingData = splits[0];
        Dataset<Row> testData = splits[1];

        System.out.println("Training set size : " + trainingData.count());
        System.out.println("Test set size     : " + testData.count());
        System.out.println();

        // =====================================================================
        // STEP 5: Configure and build the ALS model
        // =====================================================================
        // ALS Hyperparameters:
        //   - maxIter   : Maximum number of iterations (default: 10)
        //   - regParam  : Regularization parameter to prevent overfitting (λ)
        //   - rank      : Number of latent factors (dimensionality of U and V)
        //   - coldStartStrategy: How to handle unknown users/items during prediction
        //     "drop" means predictions for unknown users/items are set to NaN
        //     and dropped during evaluation.
        ALS als = new ALS()
                .setMaxIter(10)                         // 10 iterations of optimization
                .setRegParam(0.1)                       // Regularization: λ = 0.1
                .setRank(10)                            // 10 latent factors
                .setUserCol("userId")                   // Column name for user IDs
                .setItemCol("movieId")                  // Column name for item IDs
                .setRatingCol("rating")                 // Column name for ratings
                .setColdStartStrategy("drop");          // Handle cold-start problem

        System.out.println("--- Training ALS Model ---");
        System.out.println("Parameters:");
        System.out.println("  Max Iterations    : 10");
        System.out.println("  Regularization    : 0.1");
        System.out.println("  Rank (factors)    : 10");
        System.out.println("  Cold Start Strategy: drop");
        System.out.println();

        // Train the ALS model on training data
        ALSModel model = als.fit(trainingData);

        // =====================================================================
        // STEP 6: Evaluate the model on the test set using RMSE
        // =====================================================================
        // RMSE (Root Mean Squared Error) measures the average magnitude of
        // prediction errors. Lower RMSE = better model performance.
        // RMSE = sqrt( (1/n) * Σ(predicted - actual)² )
        Dataset<Row> predictions = model.transform(testData);

        System.out.println("--- Sample Predictions vs Actual ---");
        predictions.select("userId", "movieId", "rating", "prediction").show(10);

        RegressionEvaluator evaluator = new RegressionEvaluator()
                .setMetricName("rmse")                  // Use RMSE metric
                .setLabelCol("rating")                  // Actual ratings column
                .setPredictionCol("prediction");         // Predicted ratings column

        double rmse = evaluator.evaluate(predictions);

        System.out.println("============================================================");
        System.out.println("  Model Evaluation");
        System.out.println("============================================================");
        System.out.println("  Root Mean Squared Error (RMSE) = " + String.format("%.4f", rmse));
        System.out.println("  (Lower RMSE indicates better prediction accuracy)");
        System.out.println("============================================================");
        System.out.println();

        // =====================================================================
        // STEP 7: Generate Top-5 Movie Recommendations for Each User
        // =====================================================================
        // This generates the top N movies that each user is predicted to rate
        // the highest but has not yet rated.
        Dataset<Row> userRecs = model.recommendForAllUsers(5);

        System.out.println("--- Top 5 Movie Recommendations for Each User ---");
        userRecs.show(10, false);

        // =====================================================================
        // STEP 8: Generate Top-5 User Recommendations for Each Movie
        // =====================================================================
        // This generates the top N users who are predicted to rate each movie
        // the highest.
        Dataset<Row> movieRecs = model.recommendForAllItems(5);

        System.out.println("--- Top 5 User Recommendations for Each Movie ---");
        movieRecs.show(10, false);

        // =====================================================================
        // STEP 9: Save recommendations to HDFS
        // =====================================================================
        // Save user recommendations as JSON to HDFS for downstream processing.
        String userRecsPath = outputPath + "/user_recommendations";
        String movieRecsPath = outputPath + "/movie_recommendations";

        userRecs.write().mode("overwrite").json(userRecsPath);
        movieRecs.write().mode("overwrite").json(movieRecsPath);

        System.out.println("--- Results Saved to HDFS ---");
        System.out.println("User Recommendations  : " + userRecsPath);
        System.out.println("Movie Recommendations : " + movieRecsPath);
        System.out.println();

        // =====================================================================
        // STEP 10: Demonstrate recommendation for a specific user
        // =====================================================================
        // Generate recommendations for specific users (e.g., user 1 and user 2)
        Dataset<Row> specificUsers = ratings.select("userId").distinct().limit(3);
        Dataset<Row> specificUserRecs = model.recommendForUserSubset(specificUsers, 5);

        System.out.println("--- Recommendations for Specific Users ---");
        specificUserRecs.show(false);

        System.out.println("============================================================");
        System.out.println("  Recommendation System completed successfully!");
        System.out.println("============================================================");

        // =====================================================================
        // STEP 11: Stop SparkSession and release resources
        // =====================================================================
        spark.stop();
    }
}
