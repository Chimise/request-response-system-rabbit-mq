import amqp from "amqplib";
import { nanoid } from "nanoid";

export class AMQPRequest {
  constructor() {
    this.correlationMap = new Map();
  }

  async initialize() {
    this.connection = await amqp.connect("amqp://localhost");
    this.channel = await this.connection.createChannel();
    const { queue } = await this.channel.assertQueue("", { exclusive: true });
    this.replyQueue = queue;

    this.channel.consume(
      this.replyQueue,
      (msg) => {
        const correlationId = msg.properties.correlationId;
        const callback = this.correlationMap.get(correlationId);
        callback && callback(JSON.parse(msg.content.toString()));
      },
      { noAck: true }
    );
  }

  send(queue, data) {
    return new Promise((res, rej) => {
      const correlationId = nanoid();
      const timeoutId = setTimeout(() => {
        this.correlationMap.delete(correlationId);
        rej(new Error("Request timeout"));
      }, 10000);

      this.correlationMap.set(correlationId, (message) => {
        clearTimeout(timeoutId);
        this.correlationMap.delete(correlationId);
        res(message);
      });

      this.channel.sendToQueue(queue, Buffer.from(JSON.stringify(data)), {
        correlationId,
        replyTo: this.replyQueue,
      });
    });
  }

  destroy() {
    this.connection.close();
    this.channel.close();
  }
}

export class AMQPReply {
  constructor(requestQueue) {
    this.requestQueue = requestQueue;
  }

  async initialize() {
    this.connection = await amqp.connect("amqp://localhost");
    this.channel = await this.connection.createChannel();
    const { queue } = await this.channel.assertQueue(this.requestQueue);
    this.queue = queue;
  }

  handleRequests(handler) {
    this.channel.consume(this.queue, async (msg) => {
      const response = await handler(JSON.parse(msg.content.toString()));
      this.channel.sendToQueue(
        msg.properties.replyTo,
        Buffer.from(JSON.stringify(response)),
        { correlationId: msg.properties.correlationId }
      );
      this.channel.ack(msg);
    });
  }
}
