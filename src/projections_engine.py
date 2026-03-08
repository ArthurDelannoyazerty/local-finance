# File: /src/projections_engine.py

import numpy as np
import pandas as pd
import json
from dataclasses import dataclass, asdict
from typing import List, Dict, Optional

@dataclass
class ProjectionConfig:
    start_capital: float
    monthly_savings: float
    years: int
    annual_return_rate: float       # ex: 0.07 pour 7%
    inflation_rate: float           # ex: 0.02 pour 2%
    salary_growth_rate: float       # ex: 0.01 pour 1% par an
    volatility: float = 0.15        # Standard deviation pour Monte Carlo (ex: 15%)
    life_events: List[Dict] = None  # Liste d'événements [{'year': 5, 'amount': -50000, 'name': 'Apport Maison'}]

    def to_json(self):
        return json.dumps(asdict(self))

    @staticmethod
    def from_json(json_str: str):
        data = json.loads(json_str)
        return ProjectionConfig(**data)

def calculate_deterministic_projection(config: ProjectionConfig) -> pd.DataFrame:
    """Calcul linéaire classique (sans volatilité aléatoire)."""
    months = config.years * 12
    monthly_return = (1 + config.annual_return_rate) ** (1/12) - 1
    monthly_inflation = (1 + config.inflation_rate) ** (1/12) - 1
    
    data = []
    
    current_capital = config.start_capital
    current_savings = config.monthly_savings
    total_invested = config.start_capital
    
    # Gestion des événements (groupés par mois)
    events_map = {}
    if config.life_events:
        for event in config.life_events:
            m_idx = int(event['year'] * 12)
            events_map[m_idx] = events_map.get(m_idx, 0) + event['amount']

    for m in range(1, months + 1):
        # 1. Croissance du capital (Intérêts)
        current_capital *= (1 + monthly_return)
        
        # 2. Ajout de l'épargne
        current_capital += current_savings
        total_invested += current_savings
        
        # 3. Gestion événements exceptionnels
        if m in events_map:
            impact = events_map[m]
            current_capital += impact
            # On ne change pas "total_invested" pour les retraits, sauf si c'est un nouvel investissement
            if impact > 0:
                total_invested += impact

        # 4. Augmentation annuelle de l'épargne (Salaire)
        if m % 12 == 0:
            current_savings *= (1 + config.salary_growth_rate)

        # 5. Calcul Ajusté Inflation (Valeur Réelle)
        deflator = (1 + monthly_inflation) ** m
        real_capital = current_capital / deflator
        
        data.append({
            "Month": m,
            "Year": m / 12,
            "Nominal Capital": current_capital,
            "Real Capital": real_capital,
            "Total Invested": total_invested
        })
        
    return pd.DataFrame(data)

def calculate_monte_carlo(config: ProjectionConfig, n_simulations: int = 100) -> pd.DataFrame:
    """Simulation de Monte Carlo (Mouvement Brownien Géométrique)."""
    months = config.years * 12
    dt = 1/12
    mu = config.annual_return_rate
    sigma = config.volatility
    
    simulation_results = np.zeros((months, n_simulations))
    
    # Pré-calcul des événements
    events_vector = np.zeros(months)
    if config.life_events:
        for event in config.life_events:
            idx = int(event['year'] * 12) - 1
            if 0 <= idx < months:
                events_vector[idx] += event['amount']
    
    current_capitals = np.full(n_simulations, config.start_capital)
    current_savings = config.monthly_savings
    
    for m in range(months):
        # Facteur aléatoire (Normal distribution)
        shock = np.random.normal(0, 1, n_simulations)
        
        # Formule Black-Scholes discrétisée pour la croissance
        # S(t+dt) = S(t) * exp((mu - 0.5*sigma^2)*dt + sigma*sqrt(dt)*Z)
        growth_factor = np.exp((mu - 0.5 * sigma**2) * dt + sigma * np.sqrt(dt) * shock)
        
        current_capitals = current_capitals * growth_factor
        current_capitals += current_savings
        current_capitals += events_vector[m]
        
        # Sauvegarde
        simulation_results[m, :] = current_capitals
        
        # Augmentation épargne annuelle
        if (m + 1) % 12 == 0:
            current_savings *= (1 + config.salary_growth_rate)
            
    # Extraction des percentiles
    timeline = np.arange(1, months + 1) / 12
    p10 = np.percentile(simulation_results, 10, axis=1)
    p50 = np.percentile(simulation_results, 50, axis=1) # Médiane
    p90 = np.percentile(simulation_results, 90, axis=1)
    
    return pd.DataFrame({
        "Year": timeline,
        "P10 (Pessimiste)": p10,
        "P50 (Médian)": p50,
        "P90 (Optimiste)": p90
    })