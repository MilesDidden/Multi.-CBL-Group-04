from DB_utils import DBhandler
import random
import plotly.graph_objects as go
import polyline

import numpy as np
import torch
import torch.nn as nn
import torch.optim as optim
from tqdm import trange


# ==== RL Environment with Vehicle Switching ====

class VRPEnv:
    def __init__(self, jobs, vehicles):
        self.jobs = jobs
        self.n_jobs = jobs.shape[0]
        self.vehicles = vehicles
        self.n_vehicles = len(vehicles)
        self.reset()

    def reset(self):
        self.unassigned = set(range(self.n_jobs))
        self.routes = [[] for _ in self.vehicles]
        self.current_vehicle = 0
        self.current_pos = self.vehicles[self.current_vehicle]['depot'].copy()
        return self._get_state()

    def _get_state(self):
        # Concatenate current position + job mask + current vehicle one-hot
        mask = np.array([1 if i in self.unassigned else 0 for i in range(self.n_jobs)])
        vehicle_onehot = np.zeros(self.n_vehicles)
        vehicle_onehot[self.current_vehicle] = 1
        state = np.concatenate([self.current_pos / 100.0, mask, vehicle_onehot])
        return torch.tensor(state, dtype=torch.float32)

    def step(self, action):
        move_cost = 0.0

        if action == self.n_jobs:
            # Switch to next vehicle
            if self.current_vehicle + 1 >= self.n_vehicles:
                # No more vehicles → penalty!
                reward = -10.0
            else:
                # Switch vehicle → move to new depot
                move_cost = np.linalg.norm(self.current_pos - self.vehicles[self.current_vehicle]['depot'])
                self.current_vehicle += 1
                self.current_pos = self.vehicles[self.current_vehicle]['depot'].copy()
                reward = -move_cost
        else:
            job_idx = int(action)
            if job_idx not in self.unassigned:
                raise ValueError("Tried to assign already assigned job!")

            # Add to current route
            self.routes[self.current_vehicle].append(job_idx)
            job_pos = self.jobs[job_idx]
            move_cost = np.linalg.norm(self.current_pos - job_pos)

            self.current_pos = job_pos
            self.unassigned.remove(job_idx)

            reward = -move_cost

        # Check if done
        done = len(self.unassigned) == 0
        if done:
            # Return current vehicle to depot
            move_cost += np.linalg.norm(self.current_pos - self.vehicles[self.current_vehicle]['depot'])
            reward -= move_cost

        return self._get_state(), reward, done

# ==== Policy Network with Vehicle Switching ====

class PolicyNet(nn.Module):
    def __init__(self, n_jobs, n_vehicles):
        super().__init__()
        self.fc1 = nn.Linear(2 + n_jobs + n_vehicles, 256)
        self.fc2 = nn.Linear(256, n_jobs + 1)  # +1 for switch vehicle action

    def forward(self, x):
        x = torch.relu(self.fc1(x))
        logits = self.fc2(x)
        return logits

# ==== Training Loop ====

if __name__ == "__main__":

    db_handler = DBhandler("../data/", "crime_data_UK_v4.db")
    jobs = db_handler.query(
        """
        SELECT 
            lat AS latitude, 
            long AS longitude
        FROM 
            crime
        WHERE 
            lat IS NOT NULL 
            AND long IS NOT NULL
            AND ward_code = 'E05000138'
        ORDER BY 
            RANDOM()
        LIMIT 
            700;
        """
    ).to_numpy()
    db_handler.close_connection_db()

    vehicles = [
        {'depot': np.array([random.uniform(51.510, 51.525), random.uniform(-0.129, -0.105)])} for _ in range(100)
    ]

    n_jobs = jobs.shape[0]
    n_vehicles = len(vehicles)

    env = VRPEnv(jobs, vehicles)
    policy = PolicyNet(n_jobs, n_vehicles)
    optimizer = optim.Adam(policy.parameters(), lr=0.01)
    n_episodes = 500

    for episode in trange(n_episodes):
        log_probs = []
        rewards = []

        state = env.reset()
        done = False

        while not done:
            logits = policy(state)

            # Build mask for valid actions
            job_mask = torch.tensor([1 if i in env.unassigned else 0 for i in range(n_jobs)], dtype=torch.float32)
            switch_mask = torch.tensor([1.0])  # Always allow switch action

            full_mask = torch.cat([job_mask, switch_mask])
            logits = logits.masked_fill(full_mask == 0, float('-inf'))

            probs = torch.softmax(logits, dim=-1)
            dist = torch.distributions.Categorical(probs)
            action = dist.sample()

            log_prob = dist.log_prob(action)
            log_probs.append(log_prob)

            state, reward, done = env.step(action.item())
            rewards.append(reward)

        # Policy gradient update
        returns = sum(rewards)
        loss = -torch.stack(log_probs).sum() * returns

        optimizer.zero_grad()
        loss.backward()
        optimizer.step()

        if (episode + 1) % 5 == 0:
            print(f"Episode {episode+1}/{n_episodes}, Total Reward: {returns:.2f}")

    # ==== Final Solution ====

    print("Final Routes:")
    for v_idx, route in enumerate(env.routes):
        print(f"Vehicle {v_idx+1}: {route}")
