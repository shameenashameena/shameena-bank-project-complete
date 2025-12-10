import pandas as pd
df = pd.read_csv("order.csv")
revenue_per_city = df.groupby("city")["amount"].sum()
top_customers = df.groupby("customer_id")["amount"].sum().nlargest(3)


df["order_date"] = pd.to_datetime(df["order_date"])
df["week_number"] = df["order_date"].dt.isocalendar().week


filtered_df = df[(df["amount"] > 3000) & (df["city"] == "Delhi")]
filtered_df.to_csv("filtered_orders.csv", index=False)


print("Revenue per City:\n", revenue_per_city)
print("Top Customers:\n", top_customers)
print("Filtered Orders:\n", filtered_df)