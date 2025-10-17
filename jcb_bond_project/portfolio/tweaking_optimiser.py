import numpy as np

# === 1️⃣ Portfolio Simulation ===
def simulate_portfolio(cashflows_matrix, q, invest_amount=1.0):
    """Compute cumulative cashflow path for given weights."""
    cf_vector = cashflows_matrix @ q
    cumulative = np.cumsum(cf_vector) * invest_amount
    return cf_vector, cumulative


# === 2️⃣ Objective Function ===
def evaluate_portfolio(q, cashflows_matrix, target, r_pos=0.02, r_neg=-0.5):
    """Score portfolio by how well it matches target, with asymmetrical rewards."""
    portfolio_cf = cashflows_matrix @ q
    diff = portfolio_cf - target
    reward = np.where(diff >= 0, r_pos * diff, r_neg * diff)
    score = np.sum(reward)
    return score


# === 3️⃣ Tweak Step ===
def tweak_weights(q, step_size=0.01, noise_scale=0.1):
    """Randomly perturb weights (keeping them non-negative)."""
    noise = np.random.normal(0, noise_scale, size=q.shape)
    q_new = q * (1 + step_size * noise)
    q_new = np.maximum(q_new, 0)  # enforce non-negative
    return q_new / np.sum(q_new)  # normalise
