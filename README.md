# 📡 Crypto Alert System

An event-driven backend project built to explore distributed system architecture patterns in practice.

This system ingests cryptocurrency price data, processes it through asynchronous workers, and delivers intelligent alerts to subscribed users. It focuses on demonstrating real-world backend engineering concepts such as messaging, decoupling, stateful stream processing, and reliability — going beyond traditional CRUD applications.

---

## 🧠 Architecture Overview

The platform is structured around asynchronous communication using RabbitMQ topic exchanges.

```
Collector → RabbitMQ → Processing Workers → RabbitMQ → Notification Worker
                ↓                                   ↓
              Redis                             PostgreSQL
                                                    ↓
                                                   API
```

### Core Concepts Demonstrated

- Event-driven architecture
- Distributed worker communication
- Message routing via topics
- Stateful stream processing
- Runtime configuration via events
- Fault handling with DLQ
- Structured logging

---

## ⚙️ Tech Stack

- **Node.js + TypeScript**
- **RabbitMQ** — messaging backbone
- **Redis** — temporal state & caching
- **PostgreSQL** — subscriber persistence
- **Fastify** — API layer
- **Docker** — local orchestration

---

## 🔄 System Components

### Collector

- Fetches BTC price data
- Publishes `btc.price.tick` events

### Volatility Worker

- Sliding window analysis
- EMA deviation detection
- Dual EMA crossover detection
- Emits alert signals

### Telegram Worker

- Consumes alerts
- Formats user-friendly messages
- Broadcasts to subscribers

### API Service

- Manage subscriber lifecycle
- Publishes subscription events
- Persists state to PostgreSQL

---

## 📁 Project Structure

```
crypto-alert/
├── apps/
│   ├── api/          # REST API for subscriber management
│   └── worker/       # Background workers (collector, volatility, telegram)
├── packages/
│   └── shared/       # Shared types, constants, utilities
└── compose.yml       # Docker orchestration
```

---

## 📬 Event Examples

| Event                 | Description          |
| --------------------- | -------------------- |
| `btc.price.tick`      | Market data update   |
| `btc.alert.triggered` | Signal generated     |
| `subscriber.created`  | New subscriber added |
| `subscriber.removed`  | Subscriber removed   |

---

## 🚀 Running Locally

### Prerequisites

- Node.js 18+
- Docker & Docker Compose
- PostgreSQL, Redis, RabbitMQ (via Docker)

### 1. Start infrastructure

```bash
docker compose up -d
```

### 2. Configure environment

Create a `.env` file with required credentials (Telegram Bot Token, DB connection, etc.)

### 3. Start services

```bash
# Start workers
npx tsx apps/worker/src/collector.ts
npx tsx apps/worker/src/volatility-worker.ts
npx tsx apps/worker/src/telegram-worker.ts

# Start API
npx tsx apps/api/src/server.ts
```

---

## 🎯 Project Goals

This repository serves as:

- A portfolio-grade backend architecture showcase
- A sandbox for experimenting with distributed systems
- A learning environment for reliability patterns

### Focus Areas

- Messaging patterns
- State management
- Fault tolerance
- Decoupling
- Scalability

---

## 🛣 Roadmap

Planned evolutions:

- [ ] Metrics / observability layer
- [ ] Retry & backoff pipelines
- [ ] Distributed tracing
- [ ] Web dashboard visualization
- [ ] Historical analytics storage
- [ ] Event schema contracts

---

## 👤 Author

**Gustavo Aragão**
