import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime
from IPython.display import clear_output, display
import time

class PortfolioTweakingOptimiser:
    def __init__(self, prices, maturities, cashflows, dates, invest_amount,
                 r_pos=0.02, r_neg=-0.50, alpha=0.002, eta_init=0.2,
                 bond_labels=None, bond_names=None):
        """
        prices: np.array of bond prices
        maturities: list of datetime objects
        cashflows: np.array (n_dates x n_bonds)
        dates: list of datetime objects for cashflows
        invest_amount: float
        r_pos/r_neg: interest on running balance (annualised)
        alpha: floor fraction for step scaling
        eta_init: starting relative step size
        """
        self.prices = np.array(prices)
        self.maturities = maturities
        self.cashflows = np.array(cashflows)
        self.dates = np.array(dates)
        self.A = invest_amount
        self.r_pos = r_pos
        self.r_neg = r_neg
        self.alpha = alpha
        self.eta = eta_init
        self.history = []

        n = len(self.prices)
        # Ensure labels/names exist and have correct length
        if bond_labels is None or len(bond_labels) != n:
            bond_labels = [f"Bond_{i}" for i in range(n)]
        if bond_names is None or len(bond_names) != n:
            bond_names = ["Unknown"] * n
        self.bond_labels = list(bond_labels)
        self.bond_names = list(bond_names)

    def simulate_portfolio(self, q, return_path=False):
        """
        Compute final cumulative cashflow and optionally return the running balance path.

        Returns
        -------
        float | (float, pd.DataFrame)
            If return_path=False: final balance only.
            If return_path=True:  (final_balance, path_df)
        """
        import pandas as pd

        C = 0.0
        last_date = self.dates[0]
        records = []

        for t, dt in enumerate(self.dates[1:], start=1):
            year_frac = (dt - last_date).days / 365.0

            # Determine rate to apply
            rate = self.r_pos if C >= 0 else self.r_neg
            interest = C * rate * year_frac

            C += interest + np.dot(q, self.cashflows[t, :])
            records.append({
                "date": dt,
                "year_frac": year_frac,
                "rate_applied": rate,
                "interest": interest,
                "cash_inflow": np.dot(q, self.cashflows[t, :]),
                "running_balance": C
            })

            last_date = dt

            if abs(C) > 1e12:
                break  # safety overflow cap

        if not return_path:
            return C
        else:
            return C, pd.DataFrame(records)


    def plot_portfolio(self, q, iteration, score):
        """Bar chart showing bond quantities (one bar per bond) in maturity order, with outlier labels."""
        clear_output(wait=True)

        # Sort bonds by maturity
        sorted_idx = np.argsort(self.maturities)
        maturities_sorted = [self.maturities[i] for i in sorted_idx]
        q_sorted = q[sorted_idx]

        # Optional labels for x-axis
        if hasattr(self, "bond_labels"):
            labels_sorted = [self.bond_labels[i] for i in sorted_idx]
        else:
            labels_sorted = [m.strftime("%Y-%m") for m in maturities_sorted]

        plt.figure(figsize=(10, 4))
        bars = plt.bar(range(len(q_sorted)), q_sorted, color="cornflowerblue", edgecolor="black", alpha=0.85)

        # Detect outlier: any bond with size > 3x median
        median_q = np.median(q_sorted)
        outlier_idx = np.where(q_sorted > 3 * median_q)[0]

        # Highlight and annotate those
        for idx in outlier_idx:
            bars[idx].set_color("red")
            plt.text(idx, q_sorted[idx] * 1.02, labels_sorted[idx],
                     ha="center", va="bottom", fontsize=8, rotation=45, color="red")

        plt.xticks(range(len(q_sorted)), labels_sorted, rotation=45, ha="right", fontsize=8)
        plt.ylabel("Bond Quantity")
        plt.xlabel("Bonds (ordered by maturity)")
        plt.title(f"Iteration {iteration} | Final Cumulative Cashflow £{score:,.2f}")
        plt.tight_layout()
        display(plt.gcf())
        plt.close()




    def step_pairwise(self, q, η):
        """One iteration over all bond pairs."""
        n = len(q)
        donors = [j for j in range(n) if q[j] > 0]
        best_improve = 0
        best_pair = None
        best_step = 0

        F_base = self.simulate_portfolio(q)

        for i in range(n):
            for j in donors:
                if i == j: 
                    continue
                q_floor = self.alpha * self.A / (n * self.prices[i])
                h = η * max(q[i], q_floor)
                h_max = q[j] * self.prices[j] / self.prices[i]
                if h_max <= 0:
                    continue
                h = min(h, h_max)
                q_trial = q.copy()
                q_trial[i] += h
                q_trial[j] -= h * self.prices[i] / self.prices[j]
                F_trial = self.simulate_portfolio(q_trial)
                dF = F_trial - F_base
                if dF > best_improve:
                    best_improve = dF
                    best_pair = (i, j)
                    best_step = h

        return best_improve, best_pair, best_step

    def run(self, q0, max_iter=100, tol=1e-6, show_plot=True, pause_time=0.3):
        """Main optimisation loop."""
        q = q0.copy()
        total_cost = np.dot(q, self.prices)
        if total_cost <= 0:
            q = np.ones_like(q) * (self.A / len(q) / np.mean(self.prices))
        else:
            q *= self.A / total_cost

        best_score = self.simulate_portfolio(q)
        η = self.eta

        for it in range(1, max_iter + 1):
            dF, (i, j), h = self.step_pairwise(q, η)
                # --- Export text snapshot every 10 iterations ---
                        # --- Export text snapshot every 10 iterations ---
            if it % 10 == 0:
                lines = ["Bond_ISIN\tBond_Name\tQuantity\tPrice"]
                for idx in range(len(q)):
                    lines.append(
                        f"{self.bond_labels[idx]}\t{self.bond_names[idx]}\t{q[idx]:.6f}\t{self.prices[idx]:.4f}"
                    )
                filename = f"optimiser_snapshot_iter_{it:03d}.txt"
                with open(filename, "w") as f:
                    f.write("\n".join(lines))
                print(f"💾 Exported iteration {it} snapshot to {filename}")

                    # --- Export cumulative cashflow path every 10 iterations ---
            if it % 10 == 0:
                final_balance, path_df = self.simulate_portfolio(q, return_path=True)
                filename_cf = f"optimiser_cashflow_iter_{it:03d}.csv"
                path_df.to_csv(filename_cf, index=False)
                print(f"📈 Exported cashflow path for iteration {it} to {filename_cf}")



            if dF > tol and (i is not None):
                # Apply step
                q[i] += h
                q[j] -= h * self.prices[i] / self.prices[j]
                best_score += dF
                η = min(η * 1.2, 1.0)
            else:
                η *= 0.5
                if η < 1e-5:
                    break

            # Record and optionally plot
            self.history.append({"iter": it, "η": η, "dF": dF, "score": best_score})
            if show_plot and it % 1 == 0:
                self.plot_portfolio(q, it, best_score)
                time.sleep(pause_time)

        return q, best_score
