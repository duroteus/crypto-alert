# 📡 Crypto Alert System

An event-driven backend project built to explore distributed system architecture patterns in practice.

This system ingests cryptocurrency price data, processes it through asynchronous workers, and delivers intelligent alerts to subscribed users. It focuses on demonstrating real-world backend engineering concepts such as messaging, decoupling, stateful stream processing, and reliability — going beyond traditional CRUD applications.

---

## 🧠 Architecture Overview

The platform is structured around asynchronous communication using RabbitMQ topic exchanges.

```
Collector → RabbitMQ → Processing Workers → RabbitMQ → Notification Worker
    ↓           ↓              ↓                 ↓              ↓
CoinGecko    Exchanges      Redis           Exchanges     PostgreSQL
  API          DLX         (State)            DLQ            (API)
```

### Core Concepts Demonstrated

- Event-driven architecture
- Distributed worker communication
- Message routing via topics
- Stateful stream processing
- Runtime configuration via events
- Fault handling with DLQ
- Structured logging
- Circuit breaker pattern
- Multi-level retry with exponential backoff
- Alert cooldown and severity filtering

---

## ⚙️ Tech Stack

- **Node.js + TypeScript** — runtime & type safety
- **RabbitMQ** — messaging backbone with topic exchanges
- **Redis** — temporal state, EMA tracking & cooldown management
- **PostgreSQL** — subscriber persistence
- **Fastify** — API layer
- **Docker** — local orchestration
- **CoinGecko API** — real-time BTC price data

---

## 🔄 System Components

### BTC Collector (`btc-collector.ts`)

- Fetches BTC price data from CoinGecko API
- Circuit breaker pattern for fault tolerance
- Publishes `btc.price.tick` events
- Handles rate limiting and timeouts gracefully

### Volatility Worker (`btc-volatility-worker.ts`)

- Sliding window analysis (20-minute window)
- EMA deviation detection (10-period EMA)
- Dual EMA crossover detection (5/20 periods)
- Multi-level retry mechanism (5s, 30s TTL)
- Error classification (TRANSIENT, INFRA, PAYLOAD)
- Emits multiple alert types with correlation IDs

### Telegram Worker (`telegram-worker.ts`)

- Consumes alerts from RabbitMQ
- Alert severity classification (LOW, MEDIUM, HIGH)
- Cooldown system to prevent spam (10min for MEDIUM alerts)
- Formats user-friendly Markdown messages
- Broadcasts to all active subscribers

### API Service

- Manage subscriber lifecycle
- Publishes subscription events
- Persists state to PostgreSQL

---

## 🗄️ Data Storage Strategy

### Redis (Temporal State)

- **EMA Tracking**: Stores fast/slow EMA values and crossover state
- **Sliding Window**: Time-series price data with automatic expiry (20min window)
- **Cooldown Management**: Prevents alert spam with TTL-based keys (10min for MEDIUM severity)

### PostgreSQL (Persistent State)

- **Subscriber Management**: Chat IDs, preferences, subscription status
- **API Layer**: CRUD operations through Fastify endpoints

---

## 🛡️ Resilience & Error Handling

### Circuit Breaker (Collector)

- Protects against CoinGecko API failures
- Threshold: 5 failures → opens circuit
- Cooldown: 15 seconds before retry
- Prevents cascade failures

### Multi-Level Retry (Volatility Worker)

1. **First retry**: 5s TTL, transient errors
2. **Second retry**: 30s TTL, infrastructure issues  
3. **DLQ**: After 2 retries, message sent to Dead Letter Queue

Error types classified as: `TRANSIENT`, `INFRA`, `PAYLOAD`, `UNKNOWN`

### Alert Throttling (Telegram Worker)

- **LOW severity**: Never sent
- **MEDIUM severity**: 10-minute cooldown between alerts
- **HIGH severity**: Always sent immediately

---

## 📁 Project Structure

```
crypto-alert/
├── apps/
│   ├── api/                    # REST API for subscriber management
│   │   └── src/
│   │       ├── server.ts       # Fastify server
│   │       ├── db.ts           # PostgreSQL connection
│   │       ├── events.ts       # Event publishing
│   │       ├── models/
│   │       │   └── subscriber.ts
│   │       └── routes/
│   │           └── subscribers.ts
│   └── worker/                 # Background workers
│       └── src/
│           ├── btc-collector.ts         # Price data ingestion
│           ├── btc-volatility-worker.ts # Analysis & alert generation
│           ├── telegram-worker.ts       # Notification delivery
│           └── test-alert.ts            # Manual alert testing
├── packages/
│   └── shared/                 # Shared types, constants, utilities
│       ├── src/
│       │   ├── logger.ts       # Structured logging (pino)
│       │   ├── constants/
│       │   │   ├── exchanges.ts    # RabbitMQ exchange names
│       │   │   └── routing-keys.ts # Event routing keys
│       │   └── index.ts
│       └── resilience/
│           └── circuit-breaker.ts  # Circuit breaker implementation
├── compose.yml                 # Docker orchestration
├── package.json                # Workspace configuration & scripts
└── .env                        # Environment variables
```

---

## 📬 Event Examples

| Event                     | Description                      | Payload Example                          |
| ------------------------- | -------------------------------- | ---------------------------------------- |
| `btc.price.tick`          | Market data update               | `{price, ts, correlationId}`             |
| `btc.alert.triggered`     | Signal generated                 | `{type, price, correlationId, ...}`      |
| `subscriber.created`      | New subscriber added             | `{chatId, ...}`                          |
| `subscriber.removed`      | Subscriber removed               | `{chatId, ...}`                          |

### Alert Types

| Type              | Trigger Condition                                   |
| ----------------- | --------------------------------------------------- |
| `volatility`      | Price variance exceeds 0.5% in 20-minute window     |
| `ema-deviation`   | Price deviates ≥0.4% from 10-period EMA             |
| `bullish-cross`   | Fast EMA (5) crosses above Slow EMA (20)            |
| `bearish-cross`   | Fast EMA (5) crosses below Slow EMA (20)            |

---

## 🚀 Running Locally

### Prerequisites

- Node.js 18+
- Docker & Docker Compose
- npm or yarn

### 1. Install dependencies

```bash
npm install
```

### 2. Start infrastructure

```bash
docker compose up -d
```

This will start:
- **RabbitMQ** (ports 5672, 15672 for management UI)
- **PostgreSQL** (port 5432)
- **Redis** (port 6379)

### 3. Configure environment

Copy the example environment file and fill in your credentials:

```bash
cp .env.example .env
```

Then edit `.env` with your actual values:

```env
## Postgres
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=crypto_alert
POSTGRES_PASSWORD=crypto_alert
POSTGRES_DB=crypto_alert

## RabbitMQ
RABBITMQ_HOST=localhost

## Redis
REDIS_HOST=localhost
REDIS_PORT=6379

## Telegram
TELEGRAM_TOKEN=your_bot_token_here
TELEGRAM_CHAT_ID=your_chat_id

## API
API_HOST=http://localhost:3000
```

### 4. Start services

Using npm scripts (recommended):

```bash
npm run start:collector    # BTC price collector
npm run start:volatility   # Volatility analysis worker
npm run start:telegram     # Telegram notification worker
npm run start:api          # REST API server
```

Or directly with tsx:

```bash
npx tsx apps/worker/src/btc-collector.ts
npx tsx apps/worker/src/btc-volatility-worker.ts
npx tsx apps/worker/src/telegram-worker.ts
npx tsx apps/api/src/server.ts
```

---

## 🎯 Project Goals

This repository serves as:

- A portfolio-grade backend architecture showcase
- A sandbox for experimenting with distributed systems
- A learning environment for reliability patterns

### Focus Areas

- Messaging patterns (topic exchanges, DLX/DLQ)
- State management (Redis for temporal, PostgreSQL for persistent)
- Fault tolerance (circuit breakers, retries, error classification)
- Decoupling (event-driven, async workers)
- Scalability (horizontal scaling ready)

---

## 🛣 Roadmap

Planned evolutions:

- [x] Metrics / observability layer (Prometheus + Grafana)
- [ ] Distributed tracing (OpenTelemetry)
- [ ] Web dashboard visualization
- [ ] Historical analytics storage (TimescaleDB)
- [ ] Event schema contracts (JSON Schema / Avro)
- [ ] User-specific alert preferences (per subscriber)
- [ ] Multiple cryptocurrency support (ETH, SOL, etc.)
- [ ] Webhook notifications support
- [ ] Alert backtesting system
- [ ] Circuit breaker metrics & monitoring
