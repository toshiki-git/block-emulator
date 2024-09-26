import os
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

plot_save_dir = './expTest/result/figure'

if not os.path.exists(plot_save_dir):
    os.makedirs(plot_save_dir)

# Read the CSV file
file_path = './expTest/result/supervisor_measureOutput/Tx_Details.csv'  # Replace with your CSV file path
df = pd.read_csv(file_path)

# Extract the "Confirmed latency of this tx (ms)" column
latency_column = 'Confirmed latency of this tx (ms)'
latency_data_1 = df[latency_column]

# Extract the second set of data (where the 'Relay1 Tx commit timestamp (not a relay tx -> nil)' column is not empty)
timestamp_column = 'Relay1 Tx commit timestamp (not a relay tx -> nil)'
latency_data_2 = df[df[timestamp_column].notna()][latency_column]

# Extract the third set of data (where the 'Relay1 Tx commit timestamp (not a relay tx -> nil)' column is empty)
latency_data_3 = df[df[timestamp_column].isna()][latency_column]

# Set the plotting style
sns.set(style='whitegrid')

# Create a plot object
plt.figure(figsize=(10, 6))

# Plot the Kernel Density Estimation (KDE) curves
density_kwargs = {'lw': 2, 'alpha': 0.7}

# All transactions
sns.kdeplot(latency_data_1, color='blue', label='All Txs', **density_kwargs)

# Relay transactions
sns.kdeplot(latency_data_2, color='orange', label='Relay Txs (CTXs)', **density_kwargs)

# InnerShard transactions
sns.kdeplot(latency_data_3, color='green', label='InnerShard Txs', **density_kwargs)

# Add legend and title
plt.legend()
plt.title('Distribution of Confirmed Latency of This Tx (ms)')
plt.xlabel('Confirmed Latency (ms)')
plt.ylabel('Density')

# Save the plot
output_path = os.path.join(plot_save_dir, 'tx_distribution_relayMethod.png')
plt.savefig(output_path)

# Show the plot
plt.show()
