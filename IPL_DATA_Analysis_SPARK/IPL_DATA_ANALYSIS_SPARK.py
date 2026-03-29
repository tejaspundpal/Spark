# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import *

# COMMAND ----------

spark

# COMMAND ----------

from pyspark.sql import *
#create session
spark = SparkSession.builder.appName("IPL_Data_Analysis").getOrCreate()

# COMMAND ----------

ball_by_ball_schema = StructType([
    StructField("match_id", IntegerType(), True),
    StructField("over_id", IntegerType(), True),
    StructField("ball_id", IntegerType(), True),
    StructField("innings_no", IntegerType(), True),
    StructField("team_batting", StringType(), True),
    StructField("team_bowling", StringType(), True),
    StructField("striker_batting_position", IntegerType(), True),
    StructField("extra_type", StringType(), True),
    StructField("runs_scored", IntegerType(), True),
    StructField("extra_runs", IntegerType(), True),
    StructField("wides", IntegerType(), True),
    StructField("legbyes", IntegerType(), True),
    StructField("byes", IntegerType(), True),
    StructField("noballs", IntegerType(), True),
    StructField("penalty", IntegerType(), True),
    StructField("bowler_extras", IntegerType(), True),
    StructField("out_type", StringType(), True),
    StructField("caught", BooleanType(), True),
    StructField("bowled", BooleanType(), True),
    StructField("run_out", BooleanType(), True),
    StructField("lbw", BooleanType(), True),
    StructField("retired_hurt", BooleanType(), True),
    StructField("stumped", BooleanType(), True),
    StructField("caught_and_bowled", BooleanType(), True),
    StructField("hit_wicket", BooleanType(), True),
    StructField("obstructingfeild", BooleanType(), True),
    StructField("bowler_wicket", BooleanType(), True),
    StructField("match_date", DateType(), True),
    StructField("season", IntegerType(), True),
    StructField("striker", IntegerType(), True),
    StructField("non_striker", IntegerType(), True),
    StructField("bowler", IntegerType(), True),
    StructField("player_out", IntegerType(), True),
    StructField("fielders", IntegerType(), True),
    StructField("striker_match_sk", IntegerType(), True),
    StructField("strikersk", IntegerType(), True),
    StructField("nonstriker_match_sk", IntegerType(), True),
    StructField("nonstriker_sk", IntegerType(), True),
    StructField("fielder_match_sk", IntegerType(), True),
    StructField("fielder_sk", IntegerType(), True),
    StructField("bowler_match_sk", IntegerType(), True),
    StructField("bowler_sk", IntegerType(), True),
    StructField("playerout_match_sk", IntegerType(), True),
    StructField("battingteam_sk", IntegerType(), True),
    StructField("bowlingteam_sk", IntegerType(), True),
    StructField("keeper_catch", BooleanType(), True),
    StructField("player_out_sk", IntegerType(), True),
    StructField("matchdatesk", DateType(), True)
])

# COMMAND ----------

match_schema = StructType([
    StructField("match_sk", IntegerType(), True),
    StructField("match_id", IntegerType(), True),
    StructField("team1", StringType(), True),
    StructField("team2", StringType(), True),
    StructField("match_date", DateType(), True),
    StructField("season_year", IntegerType(), True),
    StructField("venue_name", StringType(), True),
    StructField("city_name", StringType(), True),
    StructField("country_name", StringType(), True),
    StructField("toss_winner", StringType(), True),
    StructField("match_winner", StringType(), True),
    StructField("toss_name", StringType(), True),
    StructField("win_type", StringType(), True),
    StructField("outcome_type", StringType(), True),
    StructField("manofmach", StringType(), True),
    StructField("win_margin", IntegerType(), True),
    StructField("country_id", IntegerType(), True)
])

# COMMAND ----------

player_schema = StructType([
    StructField("player_sk", IntegerType(), True),
    StructField("player_id", IntegerType(), True),
    StructField("player_name", StringType(), True),
    StructField("dob", DateType(), True),
    StructField("batting_hand", StringType(), True),
    StructField("bowling_skill", StringType(), True),
    StructField("country_name", StringType(), True)
])

# COMMAND ----------

player_match_schema = StructType([
    StructField("player_match_sk", IntegerType(), True),
    StructField("playermatch_key", DecimalType(), True),
    StructField("match_id", IntegerType(), True),
    StructField("player_id", IntegerType(), True),
    StructField("player_name", StringType(), True),
    StructField("dob", DateType(), True),
    StructField("batting_hand", StringType(), True),
    StructField("bowling_skill", StringType(), True),
    StructField("country_name", StringType(), True),
    StructField("role_desc", StringType(), True),
    StructField("player_team", StringType(), True),
    StructField("opposit_team", StringType(), True),
    StructField("season_year", IntegerType(), True),
    StructField("is_manofthematch", BooleanType(), True),
    StructField("age_as_on_match", IntegerType(), True),
    StructField("isplayers_team_won", BooleanType(), True),
    StructField("batting_status", StringType(), True),
    StructField("bowling_status", StringType(), True),
    StructField("player_captain", StringType(), True),
    StructField("opposit_captain", StringType(), True),
    StructField("player_keeper", StringType(), True),
    StructField("opposit_keeper", StringType(), True)
])


# COMMAND ----------


team_schema = StructType([
    StructField("team_sk", IntegerType(), True),
    StructField("team_id", IntegerType(), True),
    StructField("team_name", StringType(), True)
])

# COMMAND ----------

# ball_by_ball_df = spark.read.format("csv").option("header","true").option("inferSchema","true").load("s3://tejas-dev-bucket/Ball_By_Ball.csv")
# match_df = spark.read.format("csv").option("header","true").option("inferSchema","true").load("s3://tejas-dev-bucket/Match.csv")
# player_match_df = spark.read.format("csv").option("header","true").option("inferSchema","true").load("s3://tejas-dev-bucket/Player_match.csv")
# player_df = spark.read.format("csv").option("header","true").option("inferSchema","true").load("s3://tejas-dev-bucket/Player.csv")
# team_df = spark.read.format("csv").option("header","true").option("inferSchema","true").load("s3://tejas-dev-bucket/Team.csv")

ball_by_ball_df = spark.read.schema(ball_by_ball_schema).format("csv").option("header","true").load("s3://tejas-dev-bucket/Ball_By_Ball.csv")
match_df = spark.read.schema(match_schema).format("csv").option("header","true").load("s3://tejas-dev-bucket/Match.csv")
player_match_df = spark.read.schema(player_match_schema).format("csv").option("header","true").load("s3://tejas-dev-bucket/Player_match.csv")
player_df = spark.read.schema(player_schema).format("csv").option("header","true").load("s3://tejas-dev-bucket/Player.csv")
team_df = spark.read.schema(team_schema).format("csv").option("header","true").load("s3://tejas-dev-bucket/Team.csv")


# COMMAND ----------

ball_by_ball_df.limit(5).display()
match_df.limit(5).display()
player_df.limit(5).display()
player_match_df.limit(5).display()
team_df.limit(5).display()

# COMMAND ----------

ball_by_ball_df = ball_by_ball_df.filter((col("wides")==0) & (col("noballs")==0))
total_and_avg_runs = ball_by_ball_df.groupBy("match_id","innings_no").agg(
    sum("runs_scored").alias("total_runs"),
    avg("runs_scored").alias("average_runs")
)
total_and_avg_runs.display()

# COMMAND ----------

#window function : calculate running total of runs in each match for each over
windowSpec = Window.partitionBy("match_id","innings_no").orderBy("over_id")
ball_by_ball_df = ball_by_ball_df.withColumn("running_total_runs",sum("runs_scored").over(windowSpec))

ball_by_ball_df.select("match_id","innings_no","over_id","running_total_runs").distinct().display()

# COMMAND ----------

# Conditional column : flag for high impact balls (either wicket or more than 6 runs including extras)

ball_by_ball_df = ball_by_ball_df.withColumn(
    "high_impact",
    when(
        ((col("runs_scored") + col("extra_runs")) > 6) | (col("bowler_wicket") == True),
        True
    ).otherwise(False)
)

ball_by_ball_df.select("high_impact").display()


# COMMAND ----------

match_df.display()

# COMMAND ----------

# Extracting year, month, and day from the match date for more detailed time-based analysis

# Step 1: Extract year, month, and day from match_date
match_df_win_margin = match_df.withColumn("match_date", to_date("match_date")) \
    .withColumn("year", year("match_date")) \
    .withColumn("month", month("match_date")) \
    .withColumn("day", dayofmonth("match_date"))

# Step 2: Categorize win margins
match_df_win_margin = match_df_win_margin.withColumn(
    "win_margin_category",
    when(col("win_margin") >= 100, "High")
    .when((col("win_margin") >= 50) & (col("win_margin") < 100), "Medium")
    .otherwise("Low")
)

# Step 3: Display selected columns
match_df_win_margin.select(
    "match_date", "win_margin", "year", "month", "day", "win_margin_category"
).display()


# COMMAND ----------

# Analyze the impact of the toss: who wins the toss and the match
match_df = match_df.withColumn(
    "toss_match_winner",
    when(col("toss_winner") == col("match_winner"), "Yes").otherwise("No")
)

# Show the enhanced match DataFrame
match_df.select("match_date","toss_winner","match_winner","toss_match_winner").display()

# COMMAND ----------

#Normalize and clean player names
player_df = player_df.withColumn("player_name",lower(regexp_replace("player_name","[^a-zA-Z0-9 ]","")))

#Handle missing values in 'batting_hand' and 'bowling_skills' with default unknown
player_df = player_df.na.fill({"batting_hand":"unknown","bowling_skill":"unknown"}).replace("N/A","unknown",subset=["bowling_skill"])

#Categorizing player based on batting hand
player_df = player_df.withColumn(
    "batting_style",
    when(col("batting_hand").contains("Left"),"Left-Handed").otherwise("Right-Handed")
)

player_df.select("player_name","batting_hand","bowling_skill","batting_style").display()

# COMMAND ----------

player_df.display()

# COMMAND ----------

# Add a 'veteran_status' column based on player age
player_match_df = player_match_df.withColumn(
    "veteran_status",
    when(col("age_as_on_match") >= 35, "Veteran").otherwise("Non-Veteran")
)

# Dynamic column to calculate years since debut
player_match_df = player_match_df.withColumn(
    "years_since_debut",
    (year(current_date()) - col("season_year"))
)

player_match_df.display()

# COMMAND ----------

ball_by_ball_df.limit(5).display()
match_df.limit(5).display()
player_df.limit(5).display()
player_match_df.limit(5).display()
team_df.limit(5).display()

# COMMAND ----------

ball_by_ball_df.createOrReplaceGlobalTempView("ball_by_ball")
match_df.createOrReplaceGlobalTempView("match")
player_match_df.createOrReplaceGlobalTempView("player_match")
player_df.createOrReplaceGlobalTempView("player")
team_df.createOrReplaceGlobalTempView("team")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from global_temp.ball_by_ball limit 10

# COMMAND ----------

display(_sqldf)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT p.player_name,m.season_year,sum(b.runs_scored) as total_runs
# MAGIC FROM global_temp.ball_by_ball b
# MAGIC JOIN global_temp.match m ON b.match_id = m.match_id
# MAGIC JOIN global_temp.player_match pm ON b.striker = pm.player_id AND m.match_id = pm.match_id
# MAGIC JOIN global_temp.player p ON pm.player_id = p.player_id
# MAGIC GROUP BY m.season_year,p.player_name
# MAGIC ORDER BY total_runs DESC
# MAGIC LIMIT 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT pm.player_name,pm.season_year,sum(b.runs_scored) as total_runs
# MAGIC FROM global_temp.ball_by_ball b
# MAGIC JOIN global_temp.match m ON b.match_id = m.match_id
# MAGIC JOIN global_temp.player_match pm ON b.striker = pm.player_id AND m.match_id = pm.match_id
# MAGIC GROUP BY pm.season_year,pm.player_name
# MAGIC ORDER BY total_runs DESC
# MAGIC LIMIT 1

# COMMAND ----------

ball_by_ball_df.createOrReplaceTempView("ball_by_ball")
match_df.createOrReplaceTempView("match")
player_match_df.createOrReplaceTempView("player_match")
player_df.createOrReplaceTempView("player")
team_df.createOrReplaceTempView("team")

# COMMAND ----------

top_scoring_batsman_per_season = spark.sql("""
SELECT pm.player_name,pm.season_year,sum(b.runs_scored) as total_runs
FROM global_temp.ball_by_ball b
JOIN global_temp.match m ON b.match_id = m.match_id
JOIN global_temp.player_match pm ON b.striker = pm.player_id AND m.match_id = pm.match_id
GROUP BY pm.season_year,pm.player_name
ORDER BY total_runs DESC
LIMIT 5                                   
    """)
top_scoring_batsman_per_season.display()

# COMMAND ----------

economical_bowlers_powerplay = spark.sql("""
SELECT 
    pm.player_name, 
    pm.season_year, 
    pm.player_team,
    COUNT(b.ball_id) AS total_balls,
    SUM(b.runs_scored) AS total_runs,
    ROUND(SUM(b.runs_scored) * 6.0 / COUNT(b.ball_id), 2) AS economy_rate
FROM ball_by_ball b
JOIN match m ON b.match_id = m.match_id
JOIN player_match pm ON b.match_id = pm.match_id AND b.bowler = pm.player_id
WHERE b.over_id <= 6
GROUP BY pm.player_name, pm.season_year, pm.player_team
ORDER BY economy_rate ASC

""")
economical_bowlers_powerplay.display()

# COMMAND ----------

toss_impact_individual_matches = spark.sql("""
SELECT m.match_id, m.toss_winner, m.toss_name, m.match_winner,m.season_year,
       CASE WHEN m.toss_winner = m.match_winner THEN 'Won' ELSE 'Lost' END AS match_outcome
FROM match m
WHERE m.toss_name IS NOT NULL
ORDER BY m.match_id
""")
toss_impact_individual_matches.display()

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns

# COMMAND ----------

# Execute SQL Query
team_toss_win_performance = spark.sql("""
SELECT team1, COUNT(*) AS matches_played, SUM(CASE WHEN toss_winner = match_winner THEN 1 ELSE 0 END) AS wins_after_toss
FROM match
WHERE toss_winner = team1
GROUP BY team1
ORDER BY wins_after_toss DESC
""")

# COMMAND ----------

# Convert to Pandas DataFrame
team_toss_win_pd = team_toss_win_performance.toPandas()

# Plot
plt.figure(figsize=(12, 8))
sns.barplot(x='wins_after_toss', y='team1', data=team_toss_win_pd)
plt.title('Team Performance After Winning Toss')
plt.xlabel('Wins After Winning Toss')
plt.ylabel('Team')
plt.show()


# COMMAND ----------

#Get the top 5 players with the most appearances in matches.
player_match_df.groupBy("player_name").count().orderBy("count", ascending=False).limit(5).display()

# COMMAND ----------

#number of matches won by each team.
match_df.groupBy("match_winner").count().orderBy("count", ascending=False).display()

# COMMAND ----------

#total runs scored by each team in each season.
ball_by_ball_df.join(match_df, "match_id") \
    .join(team_df,ball_by_ball_df.team_batting == team_df.team_id,how="inner")\
    .groupBy("team_name", "season_year") \
    .agg(sum("runs_scored").alias("total_runs")) \
    .orderBy("season_year", "total_runs", ascending=False) \
    .display()

# COMMAND ----------

#top 5 batsmen with the highest strike rate (min 200 balls faced).
highest_strike_rate = ball_by_ball_df.join(player_match_df,ball_by_ball_df.striker == player_match_df.player_id,how="inner")\
                    .groupBy("striker")\
                    .agg(sum("runs_scored").alias("total_runs"),count("ball_id").alias("ball_faced"))\
                    .filter("ball_faced >= 100000")\
                    .withColumn("strike_rate",(col("total_runs")/col("ball_faced"))*100)\
                    .orderBy("strike_rate",ascending=False)    
highest_strike_rate.limit(5).display()                    

# COMMAND ----------


