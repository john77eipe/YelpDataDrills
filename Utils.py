from pyspark.sql import SparkSession
import pyspark.sql.functions as psf

def parseJSONCols(spark, df, sanitize=True, *cols):
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