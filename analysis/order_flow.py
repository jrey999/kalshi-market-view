'''
First order-flow signal: does pre-kickoff price momentum (drift toward or
away from the favorite in the window before kickoff) predict the outcome
beyond what the kickoff price level alone already predicts?

This matters because calibration.py already showed the kickoff price is
close to well-calibrated -- so a naive "does this feature correlate with
who won" test is close to meaningless, since the price already captures
most of what's knowable. The real test is whether momentum adds anything
on top of the price level: fit logit(P(favorite wins)) ~ a + b*logit(price)
as the baseline, then check whether adding drift as a second predictor
improves out-of-sample log-loss and whether its coefficient is significant.

drift_cents = favorite's filtered price at kickoff minus its filtered
price --window-hours before kickoff, on the SAME side chosen as the
favorite at kickoff (so positive = market got more confident in the
eventual kickoff favorite over the window; negative = moved away from it,
i.e. the underdog gained ground late). 24h was the first window tried and
showed nothing (see git history) -- --window-hours lets shorter,
closer-to-kickoff "steam move" horizons get tested without duplicating
this file.

Usage:
  python3 analysis/order_flow.py [--window-hours N]
'''
import argparse
import os
import sys

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from db import connect
from filters import filtered_price_at
from kickoff import load_favorites

EPS = 1e-4  # keep logit() finite at the 0/100 boundary


def logit(p):
    p = np.clip(p, EPS, 1 - EPS)
    return np.log(p / (1 - p))


def fit_logistic(X, y, iters=100, tol=1e-10):
    """IRLS/Newton-Raphson logistic regression. Returns (beta, se) with an
    intercept prepended to beta -- beta[0] is the intercept."""
    n = X.shape[0]
    X1 = np.column_stack([np.ones(n), X])
    beta = np.zeros(X1.shape[1])
    for _ in range(iters):
        z = np.clip(X1 @ beta, -30, 30)
        p_hat = 1 / (1 + np.exp(-z))
        w = np.clip(p_hat * (1 - p_hat), 1e-8, None)
        XtWX = (X1 * w[:, None]).T @ X1
        grad = X1.T @ (y - p_hat)
        step = np.linalg.solve(XtWX, grad)
        beta_new = beta + step
        if np.max(np.abs(beta_new - beta)) < tol:
            beta = beta_new
            break
        beta = beta_new
    z = np.clip(X1 @ beta, -30, 30)
    p_hat = 1 / (1 + np.exp(-z))
    w = np.clip(p_hat * (1 - p_hat), 1e-8, None)
    cov = np.linalg.inv((X1 * w[:, None]).T @ X1)
    se = np.sqrt(np.diag(cov))
    return beta, se


def predict(beta, X):
    n = X.shape[0]
    X1 = np.column_stack([np.ones(n), X])
    z = np.clip(X1 @ beta, -30, 30)
    return 1 / (1 + np.exp(-z))


def log_loss(y, p_hat):
    p_hat = np.clip(p_hat, EPS, 1 - EPS)
    return -np.mean(y * np.log(p_hat) + (1 - y) * np.log(1 - p_hat))


def brier(y, p_hat):
    return np.mean((y - p_hat) ** 2)


def kfold_indices(n, k, seed=0):
    rng = np.random.default_rng(seed)
    idx = rng.permutation(n)
    return np.array_split(idx, k)


def cv_log_loss(X, y, k=5, seed=0):
    folds = kfold_indices(len(y), k, seed)
    losses = []
    for i in range(k):
        test_idx = folds[i]
        train_idx = np.concatenate([folds[j] for j in range(k) if j != i])
        beta, _ = fit_logistic(X[train_idx], y[train_idx])
        p_hat = predict(beta, X[test_idx])
        losses.append(log_loss(y[test_idx], p_hat))
    return float(np.mean(losses))


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--window-hours", type=float, default=24, help="Lookback window before kickoff for the drift feature (default 24)")
    args = parser.parse_args()
    window_hours = args.window_hours

    con, root = connect()
    favorites, candles_by_market, stats = load_favorites(con, root)

    rows = []  # (favorite_price_kick, drift_cents, won)
    skipped_no_prior = 0
    for fav in favorites:
        candles = candles_by_market.get(fav["favorite_ticker"], [])
        price_prior, _ = filtered_price_at(candles, fav["kickoff_ts"] - window_hours * 3600)
        if price_prior is None:
            skipped_no_prior += 1
            continue
        drift = fav["favorite_price"] - price_prior
        won = 1.0 if fav["favorite_result"] == "yes" else 0.0
        rows.append((fav["favorite_price"], drift, won))

    print(f"{len(rows)} games usable ({stats['incomplete']} missing a side, {stats['tie']} exact ties, "
          f"{skipped_no_prior} with no price {window_hours}h before kickoff).\n")

    price_kick = np.array([r[0] for r in rows])
    drift = np.array([r[1] for r in rows])
    y = np.array([r[2] for r in rows])

    print(f"Drift (favorite price at kickoff minus {window_hours}h prior), cents:")
    print(f"  mean={drift.mean():+.2f}  std={drift.std():.2f}  "
          f"min={drift.min():+.0f}  max={drift.max():+.0f}")
    print(f"  moved toward favorite (>+1c): {int((drift > 1).sum())}, "
          f"away (< -1c): {int((drift < -1).sum())}, "
          f"flat: {int((np.abs(drift) <= 1).sum())}\n")

    logit_price = logit(price_kick / 100)
    drift_z = (drift - drift.mean()) / drift.std()

    X_a = logit_price.reshape(-1, 1)
    X_b = np.column_stack([logit_price, drift_z])

    beta_a, se_a = fit_logistic(X_a, y)
    beta_b, se_b = fit_logistic(X_b, y)

    print("--- Model A: price only ---")
    print(f"  intercept={beta_a[0]:+.3f} (se {se_a[0]:.3f})   "
          f"logit(price) coef={beta_a[1]:+.3f} (se {se_a[1]:.3f})")
    print(f"  in-sample log-loss={log_loss(y, predict(beta_a, X_a)):.4f}  "
          f"brier={brier(y, predict(beta_a, X_a)):.4f}")

    print(f"\n--- Model B: price + {window_hours}h drift ---")
    print(f"  intercept={beta_b[0]:+.3f} (se {se_b[0]:.3f})   "
          f"logit(price) coef={beta_b[1]:+.3f} (se {se_b[1]:.3f})   "
          f"drift coef={beta_b[2]:+.3f} (se {se_b[2]:.3f}, z={beta_b[2]/se_b[2]:+.2f})")
    print(f"  in-sample log-loss={log_loss(y, predict(beta_b, X_b)):.4f}  "
          f"brier={brier(y, predict(beta_b, X_b)):.4f}")

    cv_a = cv_log_loss(X_a, y)
    cv_b = cv_log_loss(X_b, y)
    print(f"\n--- 5-fold CV log-loss (out-of-sample) ---")
    print(f"  price only:      {cv_a:.4f}")
    print(f"  price + drift:   {cv_b:.4f}   ({'better' if cv_b < cv_a else 'worse'} than price alone)")


if __name__ == "__main__":
    main()
