/** Local, no-broker facade over the real native owner. Published default switch is held. */
import { MinnowDatabaseClient } from "./database-client.js";
import { ProtocolClient } from "./client.js";
import { OwnerUnreachableError } from "./errors.js";
import { openNativeOwner, type NativeBootOptions } from "./native-boot.js";
export class MinnowDatabase extends MinnowDatabaseClient {
  static async open(input: NativeBootOptions): Promise<MinnowDatabase> {
    const options = { ...input };
    const owner = await openNativeOwner(options),
      channel = new MessageChannel(),
      client = new ProtocolClient();
    try {
      owner.accept(channel.port2);
      await client.connect(channel.port1, options.signal);
      options.signal?.throwIfAborted();
      client.onStepDown = (reason) => client.giveUp(`The local native owner stopped (${reason})`);
      return new MinnowDatabase(client, {
        get ready() {
          return client.gaveUp
            ? Promise.reject(new OwnerUnreachableError("The local native owner stopped"))
            : Promise.resolve();
        },
        status: () => (client.gaveUp ? "unresponsive" : client.connected ? "ready" : "connecting"),
        close: async () => {
          try {
            await owner.close();
          } finally {
            channel.port1.close();
            channel.port2.close();
          }
        },
      });
    } catch (error) {
      client.close();
      channel.port1.close();
      channel.port2.close();
      await owner.close();
      throw error;
    }
  }
}
