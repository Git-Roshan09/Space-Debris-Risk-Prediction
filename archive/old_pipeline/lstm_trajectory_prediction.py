"""
Space Debris LSTM Trajectory Prediction
========================================
Uses PySpark to load and prepare data from CSV files,
then trains an LSTM model using PyTorch for trajectory prediction.

Input:  State vectors (position + velocity over time)
Output: Predicted future position (X, Y, Z)
"""

import os
import numpy as np
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, TensorDataset
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sqrt, abs as spark_abs
from pyspark.sql.window import Window
from pyspark.sql import functions as F

# ============================================================
# 1. SPARK SESSION - Load Data (Big Data Component)
# ============================================================
print("=" * 60)
print("  SPACE DEBRIS LSTM TRAJECTORY PREDICTION")
print("=" * 60)

spark = SparkSession.builder \
    .appName("SpaceDebris-LSTM-Training") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Load all state vector CSV files
print("\n[1/6] Loading state vectors with PySpark...")
df = spark.read.csv("Output/TLE_Processed/*.csv", header=True, inferSchema=True)

print(f"  Total records: {df.count()}")
print(f"  Columns: {df.columns}")
df.select("EPOCH", "POS_X", "POS_Y", "POS_Z", "VEL_X", "VEL_Y", "VEL_Z").show(5, truncate=False)

# ============================================================
# 2. FEATURE ENGINEERING WITH PYSPARK
# ============================================================
print("\n[2/6] Feature engineering with PySpark...")

# Add computed features
df = df.withColumn("ALTITUDE", sqrt(col("POS_X")**2 + col("POS_Y")**2 + col("POS_Z")**2))
df = df.withColumn("SPEED", sqrt(col("VEL_X")**2 + col("VEL_Y")**2 + col("VEL_Z")**2))

# Select numeric features and sort by epoch
feature_cols = ["POS_X", "POS_Y", "POS_Z", "VEL_X", "VEL_Y", "VEL_Z"]
features_df = df.select(*feature_cols).na.drop()

print(f"  Clean records: {features_df.count()}")
print("  Feature statistics:")
features_df.describe().show()

# ============================================================
# 3. CONVERT TO NUMPY (PySpark -> NumPy -> PyTorch)
# ============================================================
print("\n[3/6] Converting PySpark DataFrame to NumPy arrays...")

# Collect to driver (data is small enough)
data_np = np.array(features_df.collect(), dtype=np.float32)
print(f"  Data shape: {data_np.shape}")

# Normalize data (important for LSTM)
data_mean = data_np.mean(axis=0)
data_std = data_np.std(axis=0)
data_std[data_std == 0] = 1  # Avoid division by zero
data_normalized = (data_np - data_mean) / data_std

print(f"  Mean: {data_mean}")
print(f"  Std:  {data_std}")

# Create sequences for LSTM
# Input: 10 consecutive time steps -> Output: next position (X, Y, Z)
SEQUENCE_LENGTH = 10
PREDICTION_FEATURES = 3  # Predict POS_X, POS_Y, POS_Z

def create_sequences(data, seq_length):
    """Create sliding window sequences for LSTM training."""
    X, y = [], []
    for i in range(len(data) - seq_length):
        X.append(data[i:i + seq_length])           # 10 steps of [POS_X..VEL_Z]
        y.append(data[i + seq_length, :3])          # Next step's [POS_X, POS_Y, POS_Z]
    return np.array(X), np.array(y)

X, y = create_sequences(data_normalized, SEQUENCE_LENGTH)
print(f"\n  Sequences created:")
print(f"  X shape: {X.shape}  (samples, timesteps, features)")
print(f"  y shape: {y.shape}  (samples, predicted_features)")

# Split into train/test (80/20)
split_idx = int(len(X) * 0.8)
X_train, X_test = X[:split_idx], X[split_idx:]
y_train, y_test = y[:split_idx], y[split_idx:]

print(f"\n  Training set: {X_train.shape[0]} sequences")
print(f"  Test set:     {X_test.shape[0]} sequences")

# Stop Spark (no longer needed, training is on PyTorch now)
spark.stop()
print("  PySpark session closed.")

# ============================================================
# 4. LSTM MODEL DEFINITION (PyTorch)
# ============================================================
print("\n[4/6] Building LSTM model with PyTorch...")

class SpaceDebrisLSTM(nn.Module):
    """LSTM model for space debris trajectory prediction."""
    
    def __init__(self, input_size=6, hidden_size=64, num_layers=2, output_size=3, dropout=0.2):
        super(SpaceDebrisLSTM, self).__init__()
        
        self.hidden_size = hidden_size
        self.num_layers = num_layers
        
        # LSTM layers
        self.lstm = nn.LSTM(
            input_size=input_size,
            hidden_size=hidden_size,
            num_layers=num_layers,
            batch_first=True,
            dropout=dropout
        )
        
        # Fully connected layers
        self.fc1 = nn.Linear(hidden_size, 32)
        self.relu = nn.ReLU()
        self.dropout = nn.Dropout(dropout)
        self.fc2 = nn.Linear(32, output_size)
    
    def forward(self, x):
        # LSTM forward pass
        lstm_out, (h_n, c_n) = self.lstm(x)
        
        # Use last hidden state
        out = lstm_out[:, -1, :]  # Take last time step
        
        # Fully connected layers
        out = self.fc1(out)
        out = self.relu(out)
        out = self.dropout(out)
        out = self.fc2(out)
        
        return out

# Model architecture
model = SpaceDebrisLSTM(
    input_size=6,       # POS_X, POS_Y, POS_Z, VEL_X, VEL_Y, VEL_Z
    hidden_size=64,     # LSTM hidden units
    num_layers=2,       # Stacked LSTM layers
    output_size=3,      # Predict POS_X, POS_Y, POS_Z
    dropout=0.2
)

print(f"\n  Model Architecture:")
print(f"  {model}")
total_params = sum(p.numel() for p in model.parameters())
print(f"\n  Total parameters: {total_params:,}")

# ============================================================
# 5. TRAINING
# ============================================================
print("\n[5/6] Training LSTM model...")

# Convert to PyTorch tensors
X_train_tensor = torch.FloatTensor(X_train)
y_train_tensor = torch.FloatTensor(y_train)
X_test_tensor = torch.FloatTensor(X_test)
y_test_tensor = torch.FloatTensor(y_test)

# DataLoader
BATCH_SIZE = 32
EPOCHS = 50
LEARNING_RATE = 0.001

train_dataset = TensorDataset(X_train_tensor, y_train_tensor)
train_loader = DataLoader(train_dataset, batch_size=BATCH_SIZE, shuffle=True)

# Loss and optimizer
criterion = nn.MSELoss()
optimizer = torch.optim.Adam(model.parameters(), lr=LEARNING_RATE)
scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(optimizer, patience=5, factor=0.5)

# Training loop
print(f"\n  {'Epoch':<10} {'Train Loss':<15} {'Test Loss':<15} {'LR':<12}")
print("  " + "-" * 52)

train_losses = []
test_losses = []

for epoch in range(EPOCHS):
    model.train()
    epoch_loss = 0.0
    
    for batch_X, batch_y in train_loader:
        optimizer.zero_grad()
        predictions = model(batch_X)
        loss = criterion(predictions, batch_y)
        loss.backward()
        torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)
        optimizer.step()
        epoch_loss += loss.item()
    
    avg_train_loss = epoch_loss / len(train_loader)
    train_losses.append(avg_train_loss)
    
    # Test loss
    model.eval()
    with torch.no_grad():
        test_predictions = model(X_test_tensor)
        test_loss = criterion(test_predictions, y_test_tensor).item()
        test_losses.append(test_loss)
    
    scheduler.step(test_loss)
    current_lr = optimizer.param_groups[0]['lr']
    
    # Print every 5 epochs
    if (epoch + 1) % 5 == 0 or epoch == 0:
        print(f"  {epoch+1:<10} {avg_train_loss:<15.6f} {test_loss:<15.6f} {current_lr:<12.6f}")

# ============================================================
# 6. EVALUATION
# ============================================================
print("\n[6/6] Evaluating model...")

model.eval()
with torch.no_grad():
    test_pred = model(X_test_tensor).numpy()

# Denormalize predictions and actual values
test_pred_denorm = test_pred * data_std[:3] + data_mean[:3]
test_actual_denorm = y_test * data_std[:3] + data_mean[:3]

# Calculate RMSE for each coordinate
rmse_x = np.sqrt(np.mean((test_pred_denorm[:, 0] - test_actual_denorm[:, 0])**2))
rmse_y = np.sqrt(np.mean((test_pred_denorm[:, 1] - test_actual_denorm[:, 1])**2))
rmse_z = np.sqrt(np.mean((test_pred_denorm[:, 2] - test_actual_denorm[:, 2])**2))
rmse_total = np.sqrt(np.mean(np.sum((test_pred_denorm - test_actual_denorm)**2, axis=1)))

# Calculate Mean Absolute Error
mae_x = np.mean(np.abs(test_pred_denorm[:, 0] - test_actual_denorm[:, 0]))
mae_y = np.mean(np.abs(test_pred_denorm[:, 1] - test_actual_denorm[:, 1]))
mae_z = np.mean(np.abs(test_pred_denorm[:, 2] - test_actual_denorm[:, 2]))

# R² Score
ss_res = np.sum((test_actual_denorm - test_pred_denorm)**2)
ss_tot = np.sum((test_actual_denorm - np.mean(test_actual_denorm, axis=0))**2)
r2_score = 1 - (ss_res / ss_tot)

print("\n" + "=" * 60)
print("  LSTM RESULTS SUMMARY")
print("=" * 60)
print(f"""
  Position Prediction RMSE (km):
  ──────────────────────────────
    POS_X:  {rmse_x:.4f} km
    POS_Y:  {rmse_y:.4f} km
    POS_Z:  {rmse_z:.4f} km
    Total:  {rmse_total:.4f} km (3D distance error)

  Mean Absolute Error (km):
  ─────────────────────────
    MAE_X:  {mae_x:.4f} km
    MAE_Y:  {mae_y:.4f} km
    MAE_Z:  {mae_z:.4f} km

  Overall Metrics:
  ────────────────
    R² Score:       {r2_score:.4f}
    Final Train Loss: {train_losses[-1]:.6f}
    Final Test Loss:  {test_losses[-1]:.6f}
""")

# Sample predictions vs actual
print("  Sample Predictions vs Actual (km):")
print(f"  {'Actual_X':<12} {'Actual_Y':<12} {'Actual_Z':<12} {'Pred_X':<12} {'Pred_Y':<12} {'Pred_Z':<12} {'Error_3D':<12}")
print("  " + "-" * 84)
for i in range(min(10, len(test_pred_denorm))):
    error_3d = np.sqrt(np.sum((test_pred_denorm[i] - test_actual_denorm[i])**2))
    print(f"  {test_actual_denorm[i,0]:<12.2f} {test_actual_denorm[i,1]:<12.2f} {test_actual_denorm[i,2]:<12.2f} "
          f"{test_pred_denorm[i,0]:<12.2f} {test_pred_denorm[i,1]:<12.2f} {test_pred_denorm[i,2]:<12.2f} "
          f"{error_3d:<12.2f}")

# Save model
os.makedirs("Output/models", exist_ok=True)
torch.save({
    'model_state_dict': model.state_dict(),
    'data_mean': data_mean,
    'data_std': data_std,
    'sequence_length': SEQUENCE_LENGTH,
    'train_losses': train_losses,
    'test_losses': test_losses,
    'rmse_total': rmse_total,
    'r2_score': r2_score,
}, "Output/models/lstm_trajectory_model.pth")

print(f"\n  Model saved to: Output/models/lstm_trajectory_model.pth")
print("=" * 60)
