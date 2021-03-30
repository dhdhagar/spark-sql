from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.appName('spark-sql').getOrCreate()
    
    df_student = spark.read.csv("student.csv", header=True, inferSchema=True)
    df_course = spark.read.csv("course.csv", header=True, inferSchema=True)
    df_attend = spark.read.csv("attend.csv", header=True, inferSchema=True)

    df_joined = df_attend.join(df_course, df_course.COURSE_ID == df_attend.COURSE_ID).join(df_student, df_student.STUDENT_ID == df_attend.STUDENT_ID)
    df_joined.select("STUDENT_NAME").where(df_joined.COURSE_NAME=='Machine Learning').show()
    df_joined.select("STUDENT_NAME").where(df_joined.COURSE_NAME=='Machine Learning').explain()

    spark.stop()
