import pandas as pd

# Function to transform salary data to yearly values while preserving original data
def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    # Define conversion rates
    HOURS_IN_YEAR = 2080  # 40 hours per week * 52 weeks
    MONTHS_IN_YEAR = 12

    # Create new columns to store yearly salary data
    df['min_salary_yearly'] = df['min_salary']
    df['max_salary_yearly'] = df['max_salary']
    df['median_salary_yearly'] = df['median_salary']

    # Apply transformation based on salary_period column
    for index, row in df.iterrows():
        if row['salary_period'] == 'HOUR':
            df.at[index, 'min_salary_yearly'] = row['min_salary'] * HOURS_IN_YEAR
            df.at[index, 'max_salary_yearly'] = row['max_salary'] * HOURS_IN_YEAR
            df.at[index, 'median_salary_yearly'] = row['median_salary'] * HOURS_IN_YEAR
        elif row['salary_period'] == 'MONTH':
            df.at[index, 'min_salary_yearly'] = row['min_salary'] * MONTHS_IN_YEAR
            df.at[index, 'max_salary_yearly'] = row['max_salary'] * MONTHS_IN_YEAR
            df.at[index, 'median_salary_yearly'] = row['median_salary'] * MONTHS_IN_YEAR
        # If the salary_period is 'YEAR', the new columns will just hold the original values
    
    return df
