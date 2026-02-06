type State = "CLOSED" | "OPEN" | "HALF_OPEN";

export class CircuitBreaker<T> {
  private state: State = "CLOSED";
  private failures = 0;
  private nextAttempt = 0;

  constructor(
    private readonly action: () => Promise<T>,
    private readonly failureThreshold = 5,
    private readonly cooldownMs = 15000,
  ) {}

  async execute(): Promise<T> {
    const now = Date.now();

    if (this.state === "OPEN") {
      if (now > this.nextAttempt) {
        this.state = "HALF_OPEN";
      } else {
        throw new Error("CircuitOpen");
      }
    }

    try {
      const result = await this.action();

      this.failures = 0;
      this.state = "CLOSED";

      return result;
    } catch (err) {
      this.failures++;

      if (this.failures >= this.failureThreshold) {
        this.state = "OPEN";
        this.nextAttempt = now + this.cooldownMs;
      }

      throw err;
    }
  }
}
