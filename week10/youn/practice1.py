from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DoubleType

def getLevelsOfFactors(df, factors):
    levels_of_factors = {f: [row[f] for row in df.select(f).orderBy(f).distinct().collect()] for f in factors }

    return levels_of_factors

def getTargetStatsByFactors(df, factors, target):
    stats_names = ['mean', 'median', 'variance']
    output_cols = [*factors, *stats_names]

    w = Window.partitionBy(*factors).orderBy(target)
    rank_df = df.withColumn('rank', F.row_number().over(w))

    result_df = df.groupBy(*factors).agg(F.avg(target).alias('mean'),
                             F.var_pop(target).alias('variance'),
                             F.count(F.lit(1)).alias('count')) \
                        .join(rank_df, [*factors]) \
                        .filter(F.col('rank') == F.expr('count+1/2').cast(IntegerType())) \
                        .withColumn('median', F.col(target)) \
                        .orderBy(*factors) \
                        .select(*output_cols)
    return result_df


def getTargetStatsByGroups_1(df, factors, target):
    stats_names = ['mean', 'variance']
    output_cols = ['group', *stats_names]

    result_schema = StructType([
        StructField('group', StringType(), True),
        StructField('mean', DoubleType(), True),
        StructField('variance', DoubleType(), True)
    ])

    cube_df = df.cube(*factors).agg(F.avg(target).alias('mean'),
                                    F.var_pop(target).alias('variance')) \
                                .fillna('All', factors)

    levels_of_factor = getLevelsOfFactors(df, factors)
    groups = [(f, l) for f in levels_of_factors for l in levels_of_factors[f]]


    default_condi = {f: 'All' for f in factors}
    result_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), result_schema)
    for g in groups:
        condi = {**default_condi, g[0]: g[1]}
        stats_of_g = cube_df.filter(' and '.join([f'`{f}` == "{condi[f]}"' for f in condi])) \
                            .withColumn('group', F.lit(g[1])) \
                            .orderBy('group') \
                            .select(*output_cols)
        result_df = result_df.union(stats_of_g)

    return result_df

def getTargetStatsByGroups_2(df, factors, target):
    stats_names = ['mean', 'variance']
    output_cols = ['group', *stats_names]

    cube_df = df.cube(*factors).agg(F.avg(target).alias('mean'),
                                F.var_pop(target).alias('variance')) \
                            .fillna('', factors) \
                            .withColumn('group', F.concat(*factors))

    levels_of_factors = getLevelsOfFactors(df, factors)
    groups = [[l] for f in levels_of_factors for l in levels_of_factors[f]]

    group_df = spark.createDataFrame(groups, ["group"])
    result_df = group_df.join(cube_df, 'group', 'left') \
                        .orderBy('group') \
                        .select(*output_cols)

    return result_df

if __name__ == '__main__':

    spark = SparkSession.builder \
                    .appName('StudentsPerformanceAggrApp') \
                    .getOrCreate()

    data_path = 'StudentsPerformance.csv'

    df = spark.read.csv(data_path, header=True)

    df.describe().show() # describe a dataframe
    print(df.count(), len(df.columns)) # shape
    count_na_df = df.select([F.sum(F.when(F.isnan(c) | F.col(c).isNull(), 1).otherwise(0)).alias(c) for c in df.columns ]) # count nan or null
    count_na_df.show()

    factors = ['gender', 'race/ethnicity', 'parental level of education', 'lunch', 'test preparation course']
    levels_of_factors = {f: [row[f] for row in df.select(f).orderBy(f).distinct().collect()] for f in factors }
    print(levels_of_factors)

    target = 'math score'

    # Question 1
    q1_result_path = 'q1/'
    q1_result_df = getTargetStatsByFactors(df, factors, target)
    q1_result_df.coalesce(1).write.csv(q1_result_path, mode='overwrite', header=True)


    # Question 2-1.
    q21_result_path = 'q2_1/'
    q21_result_df = getTargetStatsByGroups_1(df, factors, target)
    q21_result_df.coalesce(1).write.csv(q21_result_path, mode='overwrite', header=True)

    # Question 2-1.
    q22_result_path = 'q2_2/'
    q22_result_df = getTargetStatsByGroups_2(df, factors, target)
    q22_result_df.coalesce(1).write.csv(q22_result_path, mode='overwrite', header=True)

    intersected_df = q21_result_df.intersect(q22_result_df)
    print(q21_result_df.count(), q22_result_df.count(), intersected_df.count())

    spark.stop()
