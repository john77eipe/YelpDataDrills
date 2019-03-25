import os

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as psf


os.environ["SPARK_HOME"] = "/usr/local/spark"

# running spark 2.4

def parseJSONCols(df, sanitize=True, *cols):
    """Auto infer the schema of a json column and parse into a struct.
    https://stackoverflow.com/questions/41107835/pyspark-parse-a-column-of-json-strings
    rdd-based schema inference works if you have well-formatted JSON,
    like ``{"key": "value", ...}``, but breaks if your 'JSON' is just a
    string (``"data"``) or is an array (``[1, 2, 3]``). In those cases you
    can fix everything by wrapping the data in another JSON object
    (``{"key": [1, 2, 3]}``). The ``sanitize`` option (default True)
    automatically performs the wrapping and unwrapping.

    The schema inference is based on this
    `SO Post <https://stackoverflow.com/a/45880574)/>`_.

    Parameters
    ----------
    df : pyspark dataframe
        Dataframe containing the JSON cols.
    *cols : string(s)
        Names of the columns containing JSON.
    sanitize : boolean
        Flag indicating whether you'd like to sanitize your records
        by wrapping and unwrapping them in another JSON object layer.

    Returns
    -------
    pyspark dataframe
        A dataframe with the decoded columns.
    """
    res = df
    for i in cols:

        # sanitize if requested.
        if sanitize:
            res = (
                res.withColumn(
                    i,
                    psf.concat(psf.lit('{"data": '), i, psf.lit('}'))
                )
            )
        # infer schema and apply it
        schema = spark.read.json(res.rdd.map(lambda x: x[i])).schema
        res = res.withColumn(i, psf.from_json(psf.col(i), schema))

        # unpack the wrapped object if needed
        if sanitize:
            res = res.withColumn(i, psf.col(i).data)
    return res


if __name__ == "__main__":

    # config = {
    #     "spark.executor.extraClassPath": "../extras/sqlite-jdbc-3.8.11.2.jar",
    #     "spark.driver.extraClassPath": "../extras/sqlite-jdbc-3.8.11.2.jar",
    #     "spark.jars": "../extras/sqlite-jdbc-3.8.11.2.jar"
    # }
    # conf = SparkConf()
    # for key, value in config.iteritems():
    #     conf = conf.set(key, value)
    # this conf can be added to any SparkContext

    spark = SparkSession \
        .builder \
        .appName("Yelp samples") \
        .getOrCreate()

    reviewDF = spark.read.json("/Users/johne/Downloads/yelp_dataset/review.json")
    reviewDF.printSchema()

    businessDF = spark.read.json("/Users/johne/Downloads/yelp_dataset/business.json")
    businessDF.printSchema()

    #
    # Question 1: Avg rating and ambience of businesses with star rating > 4.5
    #

    # ambienceBusinessDF = businessDF.select("business_id", "stars") \
    #     .filter("stars > 4.5") \
    #     .groupBy("business_id") \
    #     .avg("stars") \
    #     .join(businessDF, "business_id") \
    #     .filter("attributes.Ambience != '' and attributes.Ambience != 'None'") \
    #     .sort("stars") \
    #     .select("attributes.Ambience", "name", "stars")
    #
    # ambienceBusinessDF.show(10)
    # parsedAmbienceBusinessDF = parseJSONCols(ambienceBusinessDF, False, "Ambience")
    # parsedAmbienceBusinessDF.show(10)

    #
    # Question 2: Businesses review distribution for businesses star'd above 4
    #

    # goodBusinessReviewsDF = businessDF \
    #     .select("business_id", "stars", "name") \
    #     .filter("stars > 4.5") \
    #     .distinct() \
    #     .join(reviewDF, "business_id") \
    #     .select("name", "text", "useful", "funny", "cool")
    #
    #
    # goodBusinessReviewsDF.show(10)


    #
    # Question 3: Relative Frequency Distribution of aggregate review count but considering only review_counts greater than 10
    #

    totalReviews = businessDF.select("review_count") \
        .filter("review_count > 10") \
        .agg({"review_count":"sum"}).first()

    print type(totalReviews)
    print totalReviews['sum(review_count)']

    highestReviewedBusiness = businessDF.select("business_id", "review_count") \
        .filter("review_count > 10") \
        .groupBy("business_id") \
        .agg({"review_count":"sum"}) \
        .join(businessDF, "business_id") \
        .select("name", "review_count")

    highestReviewedBusiness.show()

    highestReviewedBusinessRelative = highestReviewedBusiness.withColumn("relative_reviews", highestReviewedBusiness.review_count/totalReviews)
    #
    # highestReviewedBusinessRelative.show()

    #
    # Question 4: Cities with higher (> 70%) percentage of good businesses (rated >4)
    #

    #
    # Question 5: Top % of business in each category
    #

    #
    # Question 6: Fans per user and the Interquartile range of fan count within the user's dataset
    #

    #
    # Question 7: Total checkins for each business and their
    #

    spark.stop()


