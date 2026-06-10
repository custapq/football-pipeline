import argparse
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_spark_session() -> SparkSession:
    """SparkSession แบบ local — ไม่ต้องการ GCS connector"""
    return (
        SparkSession.builder
        .appName("FootballMatchesCleaner")
        .master("local[*]")
        .getOrCreate()
    )


def flatten_matches(df):
    df_exploded = df.select(
        F.col("ingested_at"),
        F.col("execution_date"),
        F.explode("data.matches").alias("match")
    )

    return df_exploded.select(
        F.col("ingested_at"),
        F.col("execution_date"),
        F.col("match.id").alias("match_id"),
        F.col("match.utcDate").alias("match_date"),
        F.col("match.status").alias("status"),
        F.col("match.matchday").alias("matchday"),
        F.col("match.stage").alias("stage"),
        F.col("match.homeTeam.id").alias("home_team_id"),
        F.col("match.homeTeam.name").alias("home_team_name"),
        F.col("match.homeTeam.shortName").alias("home_team_short"),
        F.col("match.awayTeam.id").alias("away_team_id"),
        F.col("match.awayTeam.name").alias("away_team_name"),
        F.col("match.awayTeam.shortName").alias("away_team_short"),
        F.col("match.score.fullTime.home").alias("home_score"),
        F.col("match.score.fullTime.away").alias("away_score"),
        F.col("match.score.halfTime.home").alias("home_score_ht"),
        F.col("match.score.halfTime.away").alias("away_score_ht"),
        F.col("match.score.winner").alias("winner"),
    )


def add_derived_columns(df):
    return df.withColumn(
        "match_timestamp", F.to_timestamp("match_date")
    ).withColumn(
        "total_goals",
        F.when(
            F.col("home_score").isNotNull() & F.col("away_score").isNotNull(),
            F.col("home_score") + F.col("away_score")
        ).otherwise(F.lit(None))
    ).withColumn(
        "home_result",
        F.when(F.col("winner") == "HOME_TEAM", "WIN")
         .when(F.col("winner") == "AWAY_TEAM", "LOSS")
         .when(F.col("winner") == "DRAW", "DRAW")
         .otherwise("UNKNOWN")
    ).withColumn(
        "year", F.year("match_timestamp")
    ).withColumn(
        "month", F.month("match_timestamp")
    )


def validate_data(df) -> bool:
    total = df.count()
    if total == 0:
        logger.error("❌ No records found")
        return False

    null_match_id = df.filter(F.col("match_id").isNull()).count()
    if null_match_id > 0:
        logger.error(f"❌ Found {null_match_id} rows with null match_id")
        return False

    logger.info(f"✅ Validation passed — {total} records")
    return True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-path", required=True)   # local path
    parser.add_argument("--output-path", required=True)  # local path
    args = parser.parse_args()

    logger.info(f"Input:  {args.input_path}")
    logger.info(f"Output: {args.output_path}")

    spark = create_spark_session()

    # อ่านจาก local file
    logger.info("Reading JSON...")
    df_raw = spark.read.option("multiline", "true").json(args.input_path)

    logger.info("Flattening...")
    df_flat = flatten_matches(df_raw)

    logger.info("Adding derived columns...")
    df_final = add_derived_columns(df_flat)

    if not validate_data(df_final):
        raise ValueError("Data validation failed")

    # เขียน Parquet ลง local
    logger.info(f"Writing Parquet to {args.output_path}")
    (
        df_final
        .write
        .mode("overwrite")
        .partitionBy("year", "month")
        .parquet(args.output_path)
    )

    logger.info("✅ Spark job completed")
    spark.stop()


if __name__ == "__main__":
    main()