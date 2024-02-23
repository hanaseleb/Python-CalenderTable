import pandas as pd
import numpy as np
from datetime import datetime

# 関数の定義
def create_calender_table(start_date, end_date, first_fiscal_year=None, start_fiscal_month=None):
    # 現在日時（タイムゾーンを考慮せず）
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    
    # 会計年度の計算
    this_fy = today.year if today.month > 3 else today.year - 1
    
    # 日付の範囲を生成
    date_range = pd.date_range(start=start_date, end=end_date)
    df = pd.DataFrame(date_range, columns=['Date'])
    
    # 各種日付関連のカラムを追加
    df['Year'] = df['Date'].dt.year
    df['Month'] = df['Date'].dt.month
    df['Day'] = df['Date'].dt.day
    df['Week'] = df['Date'].dt.isocalendar().week
    df['DayOfWeek'] = df['Date'].dt.weekday + 1  # 月曜=1, 日曜=7
    df['NameOfDay'] = df['Date'].dt.strftime('%A')
    df['NameOfMonth'] = df['Date'].dt.strftime('%B')
    df['YearMonth'] = df['Year'].astype(str) + "/" + df['Month'].astype(str)
    df['RelativeDate'] = (df['Date'] - today).dt.days
    df['RelativeMonth'] = 12 * (df['Year'] - today.year) + df['Month'] - today.month
    df['RelativeYear'] = df['Year'] - today.year

    # 会計年度と会計四半期の計算
    if start_fiscal_month is not None:
        df['FY'] = np.where(df['Month'] >= start_fiscal_month, df['Year'], df['Year'] - 1)
        df['FiscalQuarter'] = ((df['Month'] - start_fiscal_month + 12) % 12) // 3 + 1
        df['FiscalQuarter_0Q'] = df['FiscalQuarter'].astype(str) + "Q"
        df['RelativeFiscalYear'] = df['FY'] - this_fy

    # 会計期間の計算
    if first_fiscal_year is not None:
        df['AccountingPeriod'] = df['Date'].apply(lambda x: x.year - first_fiscal_year if x.month >= start_fiscal_month else x.year - first_fiscal_year - 1)
        df['RelativeAccountingPeriod'] = df['AccountingPeriod'] - (today.year - first_fiscal_year)

    return df
