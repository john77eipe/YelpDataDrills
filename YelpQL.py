import os
import Utils as util
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession


os.environ["SPARK_HOME"] = "/usr/local/spark"

# running spark 2.4


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
    #reviewDF.printSchema()
    #reviewDF.show(2)
    '''
    root
    |-- business_id: string (nullable = true)
    |-- cool: long (nullable = true)
    |-- date: string (nullable = true)
    |-- funny: long (nullable = true)
    |-- review_id: string (nullable = true)
    |-- stars: double (nullable = true)
    |-- text: string (nullable = true)
    |-- useful: long (nullable = true)
    |-- user_id: string (nullable = true)

    +--------------------+----+-------------------+-----+--------------------+-----+--------------------+------+--------------------+
    |         business_id|cool|               date|funny|           review_id|stars|                text|useful|             user_id|
    +--------------------+----+-------------------+-----+--------------------+-----+--------------------+------+--------------------+
    |ujmEBvifdJM6h6RLv...|   0|2013-05-07 04:34:36|    1|Q1sbwvVQXV2734tPg...|  1.0|Total bill for th...|     6|hG7b0MtEbXx5QzbzE...|
    |NZnhc2sEQy3RmzKTZ...|   0|2017-01-14 21:30:33|    0|GJXCdrto3ASJOqKeV...|  5.0|I *adore* Travis ...|     0|yXQM5uF2jS6es16SJ...|
    +--------------------+----+-------------------+-----+--------------------+-----+--------------------+------+--------------------+
    '''

    businessDF = spark.read.json("/Users/johne/Downloads/yelp_dataset/business.json")
    #businessDF.printSchema()
    #businessDF.show(2)

    '''
    root
    |-- address: string (nullable = true)
    |-- attributes: struct (nullable = true)
    |    |-- AcceptsInsurance: string (nullable = true)
    |    |-- AgesAllowed: string (nullable = true)
    |    |-- Alcohol: string (nullable = true)
    |    |-- Ambience: string (nullable = true)
    |    |-- BYOB: string (nullable = true)
    |    |-- BYOBCorkage: string (nullable = true)
    |    |-- BestNights: string (nullable = true)
    |    |-- BikeParking: string (nullable = true)
    |    |-- BusinessAcceptsBitcoin: string (nullable = true)
    |    |-- BusinessAcceptsCreditCards: string (nullable = true)
    |    |-- BusinessParking: string (nullable = true)
    |    |-- ByAppointmentOnly: string (nullable = true)
    |    |-- Caters: string (nullable = true)
    |    |-- CoatCheck: string (nullable = true)
    |    |-- Corkage: string (nullable = true)
    |    |-- DietaryRestrictions: string (nullable = true)
    |    |-- DogsAllowed: string (nullable = true)
    |    |-- DriveThru: string (nullable = true)
    |    |-- GoodForDancing: string (nullable = true)
    |    |-- GoodForKids: string (nullable = true)
    |    |-- GoodForMeal: string (nullable = true)
    |    |-- HairSpecializesIn: string (nullable = true)
    |    |-- HappyHour: string (nullable = true)
    |    |-- HasTV: string (nullable = true)
    |    |-- Music: string (nullable = true)
    |    |-- NoiseLevel: string (nullable = true)
    |    |-- Open24Hours: string (nullable = true)
    |    |-- OutdoorSeating: string (nullable = true)
    |    |-- RestaurantsAttire: string (nullable = true)
    |    |-- RestaurantsCounterService: string (nullable = true)
    |    |-- RestaurantsDelivery: string (nullable = true)
    |    |-- RestaurantsGoodForGroups: string (nullable = true)
    |    |-- RestaurantsPriceRange2: string (nullable = true)
    |    |-- RestaurantsReservations: string (nullable = true)
    |    |-- RestaurantsTableService: string (nullable = true)
    |    |-- RestaurantsTakeOut: string (nullable = true)
    |    |-- Smoking: string (nullable = true)
    |    |-- WheelchairAccessible: string (nullable = true)
    |    |-- WiFi: string (nullable = true)
    |-- business_id: string (nullable = true)
    |-- categories: string (nullable = true)
    |-- city: string (nullable = true)
    |-- hours: struct (nullable = true)
    |    |-- Friday: string (nullable = true)
    |    |-- Monday: string (nullable = true)
    |    |-- Saturday: string (nullable = true)
    |    |-- Sunday: string (nullable = true)
    |    |-- Thursday: string (nullable = true)
    |    |-- Tuesday: string (nullable = true)
    |    |-- Wednesday: string (nullable = true)
    |-- is_open: long (nullable = true)
    |-- latitude: double (nullable = true)
    |-- longitude: double (nullable = true)
    |-- name: string (nullable = true)
    |-- postal_code: string (nullable = true)
    |-- review_count: long (nullable = true)
    |-- stars: double (nullable = true)
    |-- state: string (nullable = true)
    
    +--------------------+--------------------+--------------------+--------------------+-----------+--------------------+-------+-------------+-------------+--------------------+-----------+------------+-----+-----+
    |             address|          attributes|         business_id|          categories|       city|               hours|is_open|     latitude|    longitude|                name|postal_code|review_count|stars|state|
    +--------------------+--------------------+--------------------+--------------------+-----------+--------------------+-------+-------------+-------------+--------------------+-----------+------------+-----+-----+
    |2818 E Camino Ace...|[,,,,,,,,,,,,,,,,...|1SWheh84yJXfytovI...|   Golf, Active Life|    Phoenix|                null|      0|   33.5221425| -112.0184807|Arizona Biltmore ...|      85016|           5|  3.0|   AZ|
    |30 Eglinton Avenue W|[,, u'full_bar', ...|QXAEGFB4oINsVuTFx...|Specialty Food, R...|Mississauga|[9:0-1:0, 9:0-0:0...|      1|43.6054989743|-79.652288909|Emerald Chinese R...|    L5R 3E7|         128|  2.5|   ON|
    +--------------------+--------------------+--------------------+--------------------+-----------+--------------------+-------+-------------+-------------+--------------------+-----------+------------+-----+-----+
    '''


    #
    # Question 1: Avg rating and ambience of businesses with star rating > 4.5
    #

    ambienceBusinessDF = businessDF.select("business_id", "stars") \
        .filter("stars > 4.5") \
        .groupBy("business_id") \
        .avg("stars") \
        .join(businessDF, "business_id") \
        .filter("attributes.Ambience != '' and attributes.Ambience != 'None'") \
        .sort("stars") \
        .select("attributes.Ambience", "name", "stars")

    ambienceBusinessDF.show(10)
    # parsedAmbienceBusinessDF = util.parseJSONCols(spark, ambienceBusinessDF, False, "Ambience")
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

    # totalReviews = businessDF.select("review_count") \
    #     .filter("review_count > 10") \
    #     .agg({"review_count":"sum"}).first()
    #
    # print type(totalReviews)
    # print totalReviews['sum(review_count)']
    #
    # highestReviewedBusiness = businessDF.select("business_id", "review_count") \
    #     .filter("review_count > 10") \
    #     .groupBy("business_id") \
    #     .agg({"review_count":"sum"}) \
    #     .join(businessDF, "business_id") \
    #     .select("name", "review_count")
    #
    # highestReviewedBusiness.show()
    #
    # highestReviewedBusinessRelative = highestReviewedBusiness.withColumn("relative_reviews", highestReviewedBusiness.review_count/totalReviews)
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


