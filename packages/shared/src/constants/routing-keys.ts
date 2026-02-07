export const ROUTING_KEYS = {
  PRICE_TICK: "btc.price.tick",

  ALERT_TRIGGERED: "btc.alert.triggered",

  VOLATILITY_DLQ: "btc.volatility.dlq",
  VOLATILITY_RETRY_1: "btc.volatility.retry.1",
  VOLATILITY_RETRY_2: "btc.volatility.retry.2",

  SUBSCRIBER_CREATED: "subscriber.created",
  SUBSCRIBER_REMOVED: "subscriber.removed",
} as const;
