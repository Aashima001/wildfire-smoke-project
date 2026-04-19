"""
Wildfire & Smoke Compass — ML Models
=====================================
Random Forest + XGBoost for wildfire risk and smoke severity prediction.

Usage:
    pip install scikit-learn xgboost snowflake-connector-python pandas numpy matplotlib seaborn
    python wildfire_ml_models.py

Outputs:
    - model_results.txt       → accuracy metrics for both models
    - feature_importance.png  → feature importance bar chart
    - predictions.csv         → predictions vs actual values
    - rf_model.pkl            → saved Random Forest model
    - xgb_model.pkl           → saved XGBoost model
"""

import os
import warnings
warnings.filterwarnings("ignore")

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import seaborn as sns
import pickle

from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.model_selection import train_test_split, cross_val_score, StratifiedKFold
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.metrics import (
    accuracy_score, precision_score, recall_score, f1_score,
    classification_report, confusion_matrix,
    mean_squared_error, mean_absolute_error, r2_score
)
from sklearn.inspection import permutation_importance

import xgboost as xgb
import snowflake.connector

# ─────────────────────────────────────────────
# 1. Load data from Snowflake
# ─────────────────────────────────────────────

def load_data_from_snowflake():
    """Pull wildfire fact + dimension data from Snowflake."""
    print("Connecting to Snowflake...")

    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT", "HIPCGGG-FNB26550"),
        user=os.getenv("SNOWFLAKE_USER", "aashima1001"),
        password=os.getenv("SNOWFLAKE_PASSWORD", ""),
        database="wildfire_smoke_db",
        schema="wildfire_schema",
        warehouse="COMPUTE_WH",
    )

    query = """
        SELECT
            f.incident_name,
            cd.county,
            f.acres_burned,
            f.pct_contained,
            f.aqi_at_incident,
            f.smoke_severity_at_incident,
            f.vulnerability_score,
            f.composite_risk_score,
            f.latitude,
            f.longitude,
            cd.avg_median_income,
            cd.avg_poverty_rate,
            cd.avg_vulnerability_score,
            td.fire_year,
            td.month,
            td.is_peak_fire_season,
            wd.avg_wind_mph,
            wd.avg_fwi_proxy,
            wd.avg_aqi,
            wd.max_smoke_severity_index
        FROM Wildfire_Facts f
        LEFT JOIN County_Dimension  cd ON f.county_key  = cd.county_key
        LEFT JOIN Time_Dimension    td ON f.time_key    = td.time_key
        LEFT JOIN Weather_Dimension wd ON f.weather_key = wd.weather_key
    """

    df = pd.read_sql(query, conn)
    conn.close()
    df.columns = [c.lower() for c in df.columns]
    print(f"Loaded {len(df)} rows from Snowflake")
    return df


def load_data_fallback():
    """
    Use local CSV data if Snowflake is unavailable.
    This is the same data from your Snowflake query result.
    """
    print("Using local fallback data...")
    data = {
        "incident_name":           ["Dixie Fire","Camp Fire","Creek Fire","Caldor Fire","Thomas Fire",
                                    "Valley Fire","Summit Fire","Ridge Fire","Ember Fire","Iona Fire"],
        "county":                  ["Plumas","Butte","Madera","El Dorado","Ventura",
                                    "Fresno","San Bernardino","Riverside","Los Angeles","Los Angeles"],
        "acres_burned":            [963309, 153336, 379895, 221835, 281893, 5600, 3200, 1850, 427.5, 92.4],
        "pct_contained":           [100, 100, 100, 100, 100, 20, 30, 45, 70, 97],
        "aqi_at_incident":         [36.6, 36.6, 36.6, 36.6, 36.6, 36.6, 36.6, 36.6, 36.6, 36.6],
        "smoke_severity_at_incident": [0.1152]*10,
        "vulnerability_score":     [0.1612, 0.1419, 0.1212, 0.1173, 0.0959, 0.1327, 0.1054, 0.0979, 0.1177, 0.1177],
        "composite_risk_score":    [0.4216, 0.4177, 0.4136, 0.4128, 0.4085, 0.0855, 0.0716, 0.0654, 0.0644, 0.0632],
        "latitude":                [39.95, 39.75, 37.23, 38.61, 34.35, 36.74, 34.11, 33.72, 34.05, 34.15],
        "longitude":               [-121.01,-121.62,-119.23,-120.05,-119.14,-119.79,-117.29,-116.21,-118.24,-118.14],
        "avg_median_income":       [52000, 48000, 50000, 75000, 82000, 51000, 62000, 65000, 72000, 72000],
        "avg_poverty_rate":        [0.15, 0.17, 0.19, 0.09, 0.10, 0.20, 0.13, 0.12, 0.15, 0.15],
        "avg_vulnerability_score": [0.1612, 0.1419, 0.1212, 0.1173, 0.0959, 0.1327, 0.1054, 0.0979, 0.1177, 0.1177],
        "fire_year":               [2021, 2018, 2020, 2021, 2017, 2026, 2026, 2026, 2026, 2026],
        "month":                   [7, 11, 9, 8, 12, 4, 4, 4, 4, 4],
        "is_peak_fire_season":     [True, False, True, True, False, False, False, False, False, False],
        "avg_wind_mph":            [12.5]*10,
        "avg_fwi_proxy":           [0.208]*10,
        "avg_aqi":                 [36.6]*10,
        "max_smoke_severity_index":[0.1152]*10,
    }
    return pd.DataFrame(data)


# ─────────────────────────────────────────────
# 2. Feature engineering
# ─────────────────────────────────────────────

def engineer_features(df):
    """Create model-ready features from raw data."""
    print("\nEngineering features...")

    # Risk category label (target for classification)
    df["risk_category"] = pd.cut(
        df["composite_risk_score"],
        bins=[0, 0.10, 0.30, 1.0],
        labels=["Low", "Medium", "High"]
    )

    # Encode peak season
    df["is_peak_fire_season"] = df["is_peak_fire_season"].astype(int)

    # Log-transform acres (heavy right skew)
    df["log_acres_burned"] = np.log1p(df["acres_burned"])

    # Containment status proxy
    df["containment_ratio"] = df["pct_contained"] / 100.0

    # Encode county as numeric
    le = LabelEncoder()
    df["county_encoded"] = le.fit_transform(df["county"].astype(str))

    print(f"Risk category distribution:\n{df['risk_category'].value_counts()}")
    return df, le


def get_feature_columns():
    return [
        "log_acres_burned",
        "containment_ratio",
        "aqi_at_incident",
        "smoke_severity_at_incident",
        "vulnerability_score",
        "avg_poverty_rate",
        "avg_median_income",
        "avg_wind_mph",
        "avg_fwi_proxy",
        "is_peak_fire_season",
        "month",
        "fire_year",
        "latitude",
        "longitude",
        "county_encoded",
    ]


# ─────────────────────────────────────────────
# 3. Random Forest
# ─────────────────────────────────────────────

def train_random_forest(X_train, X_test, y_train, y_test, feature_names, task="classification"):
    """Train and evaluate a Random Forest model."""
    print(f"\n{'='*50}")
    print(f"RANDOM FOREST — {task.upper()}")
    print('='*50)

    if task == "classification":
        model = RandomForestClassifier(
            n_estimators=100,
            max_depth=5,
            min_samples_split=2,
            min_samples_leaf=1,
            random_state=42,
            class_weight="balanced",
        )
        model.fit(X_train, y_train)
        y_pred = model.predict(X_test)

        acc = accuracy_score(y_test, y_pred)
        print(f"Accuracy:  {acc:.4f}")
        print(f"\nClassification Report:\n{classification_report(y_test, y_pred, zero_division=0)}")

        metrics = {"accuracy": acc, "model": model, "predictions": y_pred}

    else:  # regression
        model = RandomForestRegressor(
            n_estimators=100,
            max_depth=5,
            min_samples_split=2,
            random_state=42,
        )
        model.fit(X_train, y_train)
        y_pred = model.predict(X_test)

        rmse = np.sqrt(mean_squared_error(y_test, y_pred))
        mae  = mean_absolute_error(y_test, y_pred)
        r2   = r2_score(y_test, y_pred)

        print(f"RMSE: {rmse:.4f}")
        print(f"MAE:  {mae:.4f}")
        print(f"R²:   {r2:.4f}")

        metrics = {"rmse": rmse, "mae": mae, "r2": r2, "model": model, "predictions": y_pred}

    # Feature importance
    importances = model.feature_importances_
    fi_df = pd.DataFrame({"feature": feature_names, "importance": importances})
    fi_df = fi_df.sort_values("importance", ascending=False)
    print(f"\nTop 5 features:\n{fi_df.head()}")

    metrics["feature_importance"] = fi_df
    return metrics


# ─────────────────────────────────────────────
# 4. XGBoost
# ─────────────────────────────────────────────

def train_xgboost(X_train, X_test, y_train, y_test, feature_names, task="classification"):
    """Train and evaluate an XGBoost model."""
    print(f"\n{'='*50}")
    print(f"XGBOOST — {task.upper()}")
    print('='*50)

    if task == "classification":
        # Encode labels to integers
        le = LabelEncoder()
        y_train_enc = le.fit_transform(y_train)
        y_test_enc  = le.transform(y_test)

        model = xgb.XGBClassifier(
            n_estimators=100,
            max_depth=4,
            learning_rate=0.1,
            subsample=0.8,
            colsample_bytree=0.8,
            use_label_encoder=False,
            eval_metric="mlogloss",
            random_state=42,
            verbosity=0,
        )
        model.fit(X_train, y_train_enc)

        y_pred_enc = model.predict(X_test)
        y_pred = le.inverse_transform(y_pred_enc)

        acc = accuracy_score(y_test, y_pred)
        print(f"Accuracy:  {acc:.4f}")
        print(f"\nClassification Report:\n{classification_report(y_test, y_pred, zero_division=0)}")

        metrics = {"accuracy": acc, "model": model, "predictions": y_pred, "label_encoder": le}

    else:  # regression
        model = xgb.XGBRegressor(
            n_estimators=100,
            max_depth=4,
            learning_rate=0.1,
            subsample=0.8,
            colsample_bytree=0.8,
            reg_alpha=0.1,   # L1
            reg_lambda=1.0,  # L2
            random_state=42,
            verbosity=0,
        )
        model.fit(X_train, y_train)

        y_pred = model.predict(X_test)

        rmse = np.sqrt(mean_squared_error(y_test, y_pred))
        mae  = mean_absolute_error(y_test, y_pred)
        r2   = r2_score(y_test, y_pred)

        print(f"RMSE: {rmse:.4f}")
        print(f"MAE:  {mae:.4f}")
        print(f"R²:   {r2:.4f}")

        metrics = {"rmse": rmse, "mae": mae, "r2": r2, "model": model, "predictions": y_pred}

    # Feature importance
    importances = model.feature_importances_
    fi_df = pd.DataFrame({"feature": feature_names, "importance": importances})
    fi_df = fi_df.sort_values("importance", ascending=False)
    print(f"\nTop 5 features:\n{fi_df.head()}")

    metrics["feature_importance"] = fi_df
    return metrics


# ─────────────────────────────────────────────
# 5. Visualizations
# ─────────────────────────────────────────────

def plot_feature_importance(rf_fi, xgb_fi, output_path="feature_importance.png"):
    """Side-by-side feature importance plots for RF and XGBoost."""
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    fig.suptitle("Feature Importance — Wildfire Risk Prediction", fontsize=14, fontweight="bold")

    colors_rf  = ["#D85A30" if i < 3 else "#F5C4B3" for i in range(len(rf_fi))]
    colors_xgb = ["#185FA5" if i < 3 else "#B5D4F4" for i in range(len(xgb_fi))]

    axes[0].barh(rf_fi["feature"][::-1], rf_fi["importance"][::-1], color=colors_rf[::-1])
    axes[0].set_title("Random Forest", fontsize=12)
    axes[0].set_xlabel("Importance")
    axes[0].tick_params(axis="y", labelsize=9)

    axes[1].barh(xgb_fi["feature"][::-1], xgb_fi["importance"][::-1], color=colors_xgb[::-1])
    axes[1].set_title("XGBoost", fontsize=12)
    axes[1].set_xlabel("Importance")
    axes[1].tick_params(axis="y", labelsize=9)

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    print(f"\nSaved: {output_path}")
    plt.close()


def plot_predictions_vs_actual(y_test, rf_preds, xgb_preds, output_path="predictions_vs_actual.png"):
    """Scatter plot of predicted vs actual composite risk scores."""
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    fig.suptitle("Predicted vs Actual — Composite Risk Score", fontsize=13, fontweight="bold")

    for ax, preds, title, color in [
        (axes[0], rf_preds,  "Random Forest", "#D85A30"),
        (axes[1], xgb_preds, "XGBoost",       "#185FA5"),
    ]:
        ax.scatter(y_test, preds, color=color, alpha=0.8, s=80, edgecolors="white", linewidth=0.5)
        mn = min(min(y_test), min(preds)) - 0.02
        mx = max(max(y_test), max(preds)) + 0.02
        ax.plot([mn, mx], [mn, mx], "k--", linewidth=1, alpha=0.5, label="Perfect prediction")
        ax.set_xlabel("Actual risk score")
        ax.set_ylabel("Predicted risk score")
        ax.set_title(title)
        ax.legend(fontsize=9)

    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches="tight")
    print(f"Saved: {output_path}")
    plt.close()


def save_results(df, rf_class, xgb_class, rf_reg, xgb_reg, feature_cols, output_path="model_results.txt"):
    """Write a summary report of all model metrics."""
    with open(output_path, "w") as f:
        f.write("=" * 60 + "\n")
        f.write("WILDFIRE & SMOKE COMPASS — ML MODEL RESULTS\n")
        f.write("=" * 60 + "\n\n")

        f.write("DATASET SUMMARY\n")
        f.write(f"  Total incidents:  {len(df)}\n")
        f.write(f"  Counties covered: {df['county'].nunique()}\n")
        f.write(f"  Features used:    {len(feature_cols)}\n")
        f.write(f"  Date range:       {df['fire_year'].min()} – {df['fire_year'].max()}\n\n")

        f.write("CLASSIFICATION — Risk Category (Low / Medium / High)\n")
        f.write("-" * 40 + "\n")
        f.write(f"  Random Forest accuracy:  {rf_class['accuracy']:.4f}\n")
        f.write(f"  XGBoost accuracy:        {xgb_class['accuracy']:.4f}\n\n")

        f.write("REGRESSION — Composite Risk Score\n")
        f.write("-" * 40 + "\n")
        f.write(f"  Random Forest  RMSE: {rf_reg['rmse']:.4f}  MAE: {rf_reg['mae']:.4f}  R²: {rf_reg['r2']:.4f}\n")
        f.write(f"  XGBoost        RMSE: {xgb_reg['rmse']:.4f}  MAE: {xgb_reg['mae']:.4f}  R²: {xgb_reg['r2']:.4f}\n\n")

        f.write("TOP 5 FEATURES (Random Forest)\n")
        f.write("-" * 40 + "\n")
        for _, row in rf_reg["feature_importance"].head(5).iterrows():
            f.write(f"  {row['feature']:<35} {row['importance']:.4f}\n")

        f.write("\nTOP 5 FEATURES (XGBoost)\n")
        f.write("-" * 40 + "\n")
        for _, row in xgb_reg["feature_importance"].head(5).iterrows():
            f.write(f"  {row['feature']:<35} {row['importance']:.4f}\n")

    print(f"Saved: {output_path}")


# ─────────────────────────────────────────────
# 6. Main
# ─────────────────────────────────────────────

def main():
    print("=" * 60)
    print("WILDFIRE & SMOKE COMPASS — ML MODELS")
    print("=" * 60)

    # Load data
    try:
        df = load_data_from_snowflake()
    except Exception as e:
        print(f"Snowflake connection failed ({e}) — using local data")
        df = load_data_fallback()

    # Feature engineering
    df, county_encoder = engineer_features(df)
    feature_cols = get_feature_columns()

    X = df[feature_cols].fillna(0)
    y_class = df["risk_category"].astype(str)
    y_reg   = df["composite_risk_score"].astype(float)

    # Train/test split
    # With only 10 rows, use leave-one-out style (test_size=0.2 = 2 rows)
    X_train, X_test, yc_train, yc_test = train_test_split(
        X, y_class, test_size=0.2, random_state=42
    )
    _, _, yr_train, yr_test = train_test_split(
        X, y_reg, test_size=0.2, random_state=42
    )

    print(f"\nTrain size: {len(X_train)} | Test size: {len(X_test)}")

    # ── Train all 4 models ──────────────────────────────────────
    rf_class  = train_random_forest(X_train, X_test, yc_train, yc_test, feature_cols, "classification")
    xgb_class = train_xgboost(X_train, X_test, yc_train, yc_test, feature_cols, "classification")
    rf_reg    = train_random_forest(X_train, X_test, yr_train, yr_test, feature_cols, "regression")
    xgb_reg   = train_xgboost(X_train, X_test, yr_train, yr_test, feature_cols, "regression")

    # ── Save models ─────────────────────────────────────────────
    with open("rf_model.pkl", "wb") as f:
        pickle.dump(rf_reg["model"], f)
    with open("xgb_model.pkl", "wb") as f:
        pickle.dump(xgb_reg["model"], f)
    print("\nSaved: rf_model.pkl")
    print("Saved: xgb_model.pkl")

    # ── Save predictions CSV ────────────────────────────────────
    pred_df = pd.DataFrame({
        "actual_risk_score":   yr_test.values,
        "rf_predicted":        rf_reg["predictions"].round(4),
        "xgb_predicted":       xgb_reg["predictions"].round(4),
        "actual_risk_category":yc_test.values,
        "rf_category_pred":    rf_class["predictions"],
        "xgb_category_pred":   xgb_class["predictions"],
    })
    pred_df.to_csv("predictions.csv", index=False)
    print("Saved: predictions.csv")

    # ── Plots ───────────────────────────────────────────────────
    plot_feature_importance(
        rf_reg["feature_importance"],
        xgb_reg["feature_importance"]
    )
    plot_predictions_vs_actual(
        yr_test.values,
        rf_reg["predictions"],
        xgb_reg["predictions"]
    )

    # ── Summary report ──────────────────────────────────────────
    save_results(df, rf_class, xgb_class, rf_reg, xgb_reg, feature_cols)

    print("\n" + "=" * 60)
    print("ALL DONE — output files:")
    print("  model_results.txt")
    print("  feature_importance.png")
    print("  predictions_vs_actual.png")
    print("  predictions.csv")
    print("  rf_model.pkl")
    print("  xgb_model.pkl")
    print("=" * 60)


if __name__ == "__main__":
    main()
