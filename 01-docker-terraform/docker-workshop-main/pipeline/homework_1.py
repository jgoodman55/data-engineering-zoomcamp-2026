import pandas as pd

green_taxi_data = pd.read_parquet("green_tripdata_2025-11.parquet")
# print(green_taxi_data.head())

# print(green_taxi_data['trip_distance'].describe())

# For the trips in November 2025 (lpep_pickup_datetime between '2025-11-01' and '2025-12-01', exclusive of the upper bound), how many trips had a trip_distance of less than or equal to 1 mile?
filtered_data = green_taxi_data[
    (green_taxi_data["lpep_pickup_datetime"] >= "2025-11-01") &
    (green_taxi_data["lpep_pickup_datetime"] < "2025-12-01") &
    (green_taxi_data["trip_distance"] <= 1)
]
# print(f"Number of trips in November 2025 with trip_distance <= 1 mile: {len(filtered_data)}")

# Which was the pick up day with the longest trip distance? Only consider trips with trip_distance less than 100 miles (to exclude data errors).

# Use the pick up time for your calculations.

valid_trips = green_taxi_data[green_taxi_data["trip_distance"] < 100]

# print(valid_trips.head())

# print(valid_trips[valid_trips["trip_distance"] == valid_trips["trip_distance"].max()]["lpep_pickup_datetime"])

# print(valid_trips[valid_trips["trip_distance"] == valid_trips["trip_distance"].max()])

# test_date = '2025-11-14'
# print(
#     max(green_taxi_data[
#         (green_taxi_data["lpep_pickup_datetime"] >= pd.Timestamp("2025-11-14")) &
#         (green_taxi_data["lpep_pickup_datetime"] <  pd.Timestamp("2025-11-15"))
#     ]['trip_distance'])
# )

# print(
#     max(valid_trips[
#         (valid_trips["lpep_pickup_datetime"] >= pd.Timestamp("2025-11-20")) &
#         (valid_trips["lpep_pickup_datetime"] <  pd.Timestamp("2025-11-21"))
#     ]['trip_distance'])
# )

# print(
#     max(green_taxi_data[
#         (green_taxi_data["lpep_pickup_datetime"] >= pd.Timestamp("2025-11-23")) &
#         (green_taxi_data["lpep_pickup_datetime"] <  pd.Timestamp("2025-11-24"))
#     ]['trip_distance'])
# )

# print(
#     max(green_taxi_data[
#         (green_taxi_data["lpep_pickup_datetime"] >= pd.Timestamp("2025-11-25")) &
#         (green_taxi_data["lpep_pickup_datetime"] <  pd.Timestamp("2025-11-26"))
#     ]['trip_distance'])
# )

# 2025-11-14
# 2025-11-20
# 2025-11-23
# 2025-11-25



taxi_zone_lookup = pd.read_csv('taxi_zone_lookup.csv')
# print(taxi_zone_lookup.head())
# print(green_taxi_data.columns)

merged_df =  pd.merge(
    green_taxi_data,
    taxi_zone_lookup,
    left_on='PULocationID',      # Column name in df1
    right_on='LocationID', # Column name in df2
    how='left'          # Specify a left join
)

# print(merged_df[['PULocationID','LocationID']])

# print(
#     len(
#         merged_df[
#             (merged_df['lpep_pickup_datetime'] >= pd.Timestamp("2025-11-18")) &
#             (merged_df['lpep_pickup_datetime'] <  pd.Timestamp("2025-11-19"))
#         ].groupby('')
#     )
# )


# Which was the pickup zone with the largest total_amount (sum of all trips) on November 18th, 2025?

# East Harlem North
# East Harlem South
# Morningside Heights
# Forest Hills

trip_counts = (
    merged_df[
            (merged_df['lpep_pickup_datetime'] >= pd.Timestamp("2025-11-18")) &
            (merged_df['lpep_pickup_datetime'] <  pd.Timestamp("2025-11-19"))
        ]
    .groupby("Zone")
    .size()
    .reset_index(name="trip_count")
)

# print(trip_counts[trip_counts["trip_count"] == trip_counts["trip_count"].max()])

# print(trip_counts[trip_counts["Zone"]=='East Harlem South'])
# print(trip_counts[trip_counts["Zone"]=='East Harlem North'])
# print(trip_counts[trip_counts["Zone"]=='Morningside Heights'])
# print(trip_counts[trip_counts["Zone"]=='Forest Hills'])


# Question 6. For the passengers picked up in the zone named "East Harlem North" in November 2025, which was the drop off zone that had the largest tip? 


merged_df = merged_df.merge(
    taxi_zone_lookup,
    left_on="DOLocationID",
    right_on="LocationID",
    how="left",
    suffixes=("", "_dropoff")
)

question_6_df = merged_df[
        (merged_df['lpep_pickup_datetime'] >= pd.Timestamp("2025-11-01")) &
        (merged_df['lpep_pickup_datetime'] <  pd.Timestamp("2025-12-01")) &
        (merged_df['Zone']=="East Harlem North")
    ]

trip_counts = (
    question_6_df
    .groupby("Zone_dropoff", as_index=False)
    .agg(max_tip_amount=("tip_amount", "max"))
)

# JFK Airport
# Yorkville West
# East Harlem North
# LaGuardia Airport

print(trip_counts[trip_counts["max_tip_amount"] == trip_counts["max_tip_amount"].max()])

print(trip_counts[trip_counts["Zone_dropoff"] == 'JFK Airport'])
print(trip_counts[trip_counts["Zone_dropoff"] == 'Yorkville West'])
print(trip_counts[trip_counts["Zone_dropoff"] == 'East Harlem North'])
print(trip_counts[trip_counts["Zone_dropoff"] == 'LaGuardia Airport'])