from DB_utils import DBhandler
import random
import plotly.graph_objects as go

import numpy as np
import torch
import torch.nn as nn
import torch.optim as optim
from tqdm import trange


# ==== RL Environment with Vehicle Switching + Real Map Support + Vehicle Capacity ====

class VRPEnv:
    def __init__(self, jobs, vehicles, vehicle_capacity):
        self.jobs = jobs.astype(np.float32)
        self.n_jobs = jobs.shape[0]
        self.vehicles = vehicles
        self.n_vehicles = len(vehicles)
        self.vehicle_capacity = vehicle_capacity
        self.reset()

    def reset(self):
        self.unassigned = set(range(self.n_jobs))
        self.routes = [[] for _ in self.vehicles]
        self.current_vehicle = 0
        self.current_pos = self.vehicles[self.current_vehicle]['depot'].copy()
        self.vehicle_load = 0
        return self._get_state()

    def _get_state(self):
        mask = np.array([1 if i in self.unassigned else 0 for i in range(self.n_jobs)])
        vehicle_onehot = np.zeros(self.n_vehicles)
        vehicle_onehot[self.current_vehicle] = 1
        state = np.concatenate([self.current_pos / 100.0, mask, vehicle_onehot])
        return torch.tensor(state, dtype=torch.float32)

    def step(self, action):
        move_cost = 0.0

        if action == self.n_jobs:
            if self.current_vehicle + 1 >= self.n_vehicles:
                reward = -10.0
            else:
                move_cost = haversine(
                    self.current_pos[0], self.current_pos[1],
                    self.vehicles[self.current_vehicle]['depot'][0],
                    self.vehicles[self.current_vehicle]['depot'][1]
                )
                self.current_vehicle += 1
                self.current_pos = self.vehicles[self.current_vehicle]['depot'].copy()
                self.vehicle_load = 0
                reward = -move_cost
        else:
            job_idx = int(action)
            if job_idx not in self.unassigned:
                raise ValueError("Tried to assign already assigned job!")

            self.routes[self.current_vehicle].append(job_idx)
            job_pos = self.jobs[job_idx]

            move_cost = haversine(
                self.current_pos[0], self.current_pos[1],
                job_pos[0], job_pos[1]
            )

            self.current_pos = job_pos
            self.unassigned.remove(job_idx)
            self.vehicle_load += 1

            reward = -move_cost

            # Force vehicle switch if capacity full
            if self.vehicle_load >= self.vehicle_capacity and self.unassigned:
                if self.current_vehicle + 1 < self.n_vehicles:
                    move_cost = haversine(
                        self.current_pos[0], self.current_pos[1],
                        self.vehicles[self.current_vehicle]['depot'][0],
                        self.vehicles[self.current_vehicle]['depot'][1]
                    )
                    self.current_vehicle += 1
                    self.current_pos = self.vehicles[self.current_vehicle]['depot'].copy()
                    self.vehicle_load = 0
                    reward -= move_cost

        done = len(self.unassigned) == 0
        if done:
            move_cost = haversine(
                self.current_pos[0], self.current_pos[1],
                self.vehicles[self.current_vehicle]['depot'][0],
                self.vehicles[self.current_vehicle]['depot'][1]
            )
            reward -= move_cost

            reward += self.current_vehicle * 500

        return self._get_state(), reward, done

# ==== Combined Policy + Value Network ====

class PolicyValueNet(nn.Module):
    def __init__(self, n_jobs, n_vehicles):
        super().__init__()
        self.fc1 = nn.Linear(2 + n_jobs + n_vehicles, 256)
        self.fc_policy = nn.Linear(256, n_jobs + 1)
        self.fc_value = nn.Linear(256, 1)

    def forward(self, x):
        x = torch.relu(self.fc1(x))
        logits = self.fc_policy(x)
        value = self.fc_value(x).squeeze(-1)
        return logits, value

# ==== Training Loop ====

if __name__ == "__main__":

    from DB_utils import DBhandler
    import random
    import numpy as np
    import torch
    import torch.optim as optim
    from tqdm import trange

    def haversine(lat1, lon1, lat2, lon2):
        R = 6371e3
        phi1 = np.radians(lat1)
        phi2 = np.radians(lat2)
        delta_phi = np.radians(lat2 - lat1)
        delta_lambda = np.radians(lon2 - lon1)

        a = np.sin(delta_phi / 2) ** 2 + np.cos(phi1) * np.cos(phi2) * np.sin(delta_lambda / 2) ** 2
        c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))

        return R * c

    job_schedule = [700]  # skip curriculum
    job_threshold = -1000
    curriculum_idx = 0

    n_vehicles = 100
    vehicle_capacity = 20

    env = None
    policy_value_net = PolicyValueNet(n_jobs=job_schedule[curriculum_idx], n_vehicles=n_vehicles)
    optimizer = optim.Adam(policy_value_net.parameters(), lr=1e-3)

    gamma = 0.99
    n_episodes = 2000

    for episode in trange(n_episodes):

        if env is None or env.n_jobs != job_schedule[curriculum_idx]:
            print(f"\n=== Curriculum Stage {curriculum_idx+1} → {job_schedule[curriculum_idx]} jobs ===")
            db_handler = DBhandler("../data/", "crime_data_UK_v4.db")
            jobs = db_handler.query(
                f"""
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
                    {job_schedule[curriculum_idx]};
                """
            ).to_numpy().astype(np.float32)
            db_handler.close_connection_db()

            vehicles = [
                {'depot': np.array([random.uniform(51.510, 51.525), random.uniform(-0.129, -0.105)], dtype=np.float32)} for _ in range(n_vehicles)
            ]

            env = VRPEnv(jobs, vehicles, vehicle_capacity)
            n_jobs = env.n_jobs

            policy_value_net = PolicyValueNet(n_jobs, n_vehicles)
            optimizer = optim.Adam(policy_value_net.parameters(), lr=1e-3)

        log_probs = []
        values = []
        rewards = []

        state = env.reset()
        done = False

        while not done:
            logits, value = policy_value_net(state)

            job_mask = torch.tensor([1 if i in env.unassigned else 0 for i in range(n_jobs)], dtype=torch.float32)
            switch_mask = torch.tensor([1.0])

            full_mask = torch.cat([job_mask, switch_mask])
            logits = logits.masked_fill(full_mask == 0, float('-inf'))

            probs = torch.softmax(logits, dim=-1)
            dist = torch.distributions.Categorical(probs)
            action = dist.sample()

            log_prob = dist.log_prob(action)
            log_probs.append(log_prob)
            values.append(value)

            state, reward, done = env.step(action.item())
            rewards.append(reward)

        returns = []
        G = 0
        for r in reversed(rewards):
            G = r + gamma * G
            returns.insert(0, G)
        returns = torch.tensor(returns)
        returns = (returns - returns.mean()) / (returns.std() + 1e-8)

        values = torch.stack(values)
        log_probs = torch.stack(log_probs)

        advantage = returns - values.detach()

        policy_loss = -torch.sum(log_probs * advantage)
        value_loss = torch.nn.functional.mse_loss(values, returns)
        loss = policy_loss + value_loss

        optimizer.zero_grad()
        loss.backward()
        optimizer.step()

        total_reward = sum(rewards)
        if (episode + 1) % 50 == 0:
            print(f"Episode {episode+1}/{n_episodes}, Total Reward: {total_reward:.2f}, Value Loss: {value_loss.item():.2f}")

        if total_reward > job_threshold and curriculum_idx < len(job_schedule) - 1:
            curriculum_idx += 1
            print(f"\n>> Progressing to next curriculum stage: {job_schedule[curriculum_idx]} jobs <<")

    # ==== Plot Final Routes ====

    print("\n=== Plotting Final Routes ===")
    fig = go.Figure()

    for v_idx, v in enumerate(env.vehicles):
        depot = v['depot']
        fig.add_trace(go.Scattermap(
            lat=[depot[0]],
            lon=[depot[1]],
            mode='markers',
            marker=dict(size=12, color='red'),
            name=f'Depot V{v_idx+1}'
        ))

    fig.add_trace(go.Scattermap(
        lat=env.jobs[:, 0],
        lon=env.jobs[:, 1],
        mode='markers',
        marker=dict(size=8, color='blue'),
        name='Jobs'
    ))

    for v_idx, route in enumerate(env.routes):
        if route:
            lat = [env.vehicles[v_idx]['depot'][0]] + [env.jobs[j][0] for j in route] + [env.vehicles[v_idx]['depot'][0]]
            lon = [env.vehicles[v_idx]['depot'][1]] + [env.jobs[j][1] for j in route] + [env.vehicles[v_idx]['depot'][1]]

            fig.add_trace(go.Scattermap(
                lat=lat,
                lon=lon,
                mode='lines+markers',
                line=dict(width=2),
                name=f'Route V{v_idx+1}'
            ))

    fig.update_layout(
        mapbox_style="carto-positron",
        mapbox_zoom=12,
        mapbox_center={"lat": 51.517, "lon": -0.117},
        margin={"r":0,"t":0,"l":0,"b":0}
    )

    fig.write_html("RL_route_optimization.html", auto_open=True)
