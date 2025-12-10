import numpy as np
import pandas as pd


data = np.array([4.1, 4.3, np.nan, 4.5, 4.4, np.nan, 4.6])

mean_val = np.nanmean(data)
data_cleaned = np.where(np.isnan(data), mean_val, data)

mean = np.mean(data_cleaned)
std = np.std(data_cleaned)

rolling_avg = pd.Series(data_cleaned).rolling(3).mean().to_numpy()

# Anomalies
anomalies = data_cleaned[data_cleaned > mean + 2*std]

print("Cleaned Data:", data_cleaned)
print("Mean:", mean)
print("Std:", std)
print("Rolling Avg:", rolling_avg)
print("Anomalies:", anomalies)