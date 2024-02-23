from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_date, datediff, months_between, trunc, floor
import pyspark.sql.functions as F
from datetime import datetime, timedelta
from pyspark.sql.types import DateType

def create_calendar_table_spark(start_date, end_date, first_fiscal_year=None, start_fiscal_month=None):

    # 文字列をdatetimeオブジェクトに変換
    start_date = datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.strptime(end_date, '%Y-%m-%d')

    # 日付の範囲を生成
    date_range = [start_date + timedelta(days=x) for x in range((end_date - start_date).days + 1)]
    df = spark.createDataFrame(date_range, DateType()).toDF("Date")

    # 現在日時（タイムゾーンを考慮せず）
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    # 相対月計算用
    first_day_of_current_month = today.replace(day=1)

    # 会計年度の計算のための基準
    this_fy = today.year if today.month > 3 else today.year - 1

    # 各種日付関連のカラムを追加
    df = df.withColumn("Year", year("Date"))\
           .withColumn("Month", month("Date"))\
           .withColumn("Day", dayofmonth("Date"))\
           .withColumn("Week", weekofyear("Date"))\
           .withColumn("DayOfWeek", dayofweek("Date"))\
           .withColumn("NameOfDay", date_format("Date", 'EEEE'))\
           .withColumn("NameOfMonth", date_format("Date", 'MMMM'))\
           .withColumn("YearMonth", F.concat_ws("/", col("Year"), col("Month")))\
           .withColumn("RelativeDate", datediff("Date", lit(today)))\
           .withColumn("RelativeMonth", floor(months_between("Date", lit(first_day_of_current_month))))\
           .withColumn("RelativeYear", col("Year") - year(lit(today)))

    # 会計年度と会計四半期の計算
    if start_fiscal_month is not None:
        fiscal_year_expr = F.when(month("Date") >= start_fiscal_month, year("Date")).otherwise(year("Date") - 1)
        df = df.withColumn("FY", fiscal_year_expr)\
            .withColumn("FiscalQuarter", expr(f"floor(((month(Date) - {start_fiscal_month} + 12) % 12) / 3) + 1"))\
            .withColumn("FiscalQuarter_0Q", F.concat(col("FiscalQuarter"), lit("Q")))\
            .withColumn("RelativeFiscalYear", col("FY") - this_fy)

    # 'AccountingPeriod' 列と 'RelativeAccountingPeriod' 列を追加
    if first_fiscal_year is not None and start_fiscal_month is not None:
        accounting_period_expr = F.when(month("Date") >= start_fiscal_month, year("Date") - first_fiscal_year).otherwise(year("Date") - first_fiscal_year - 1)
        df = df.withColumn("AccountingPeriod", accounting_period_expr)

        current_accounting_period = today.year - first_fiscal_year if today.month >= start_fiscal_month else today.year - first_fiscal_year - 1
        df = df.withColumn("RelativeAccountingPeriod", col("AccountingPeriod") - current_accounting_period)

    return df
