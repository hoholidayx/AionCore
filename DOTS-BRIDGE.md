# Configure Dots in Aion Web

AionCore reads `dots-bridge.json` from its data directory at startup. A standalone Web installation defaults to
`~/.aionui-web`. Use the Bridge's private application API. A remote Bridge can be
reached through a local loopback relay, either through a NAS with TLS and
Tailscale or through SSH port forwarding. See the Bridge repository's deployment
guide for both routes.

```json
{
  "url": "http://127.0.0.1:18765",
  "token_file": "dots-bridge.token",
  "channel": "default"
}
```

Create the token file with owner-only permissions and put the Bridge
application API key in it. A relative path is resolved from the AionCore data
directory. Optional `owner_id` selects an Aion user by ID. By default, AionCore
uses the primary WebUI user. Optional `subscription_id` selects one active Dot
when the Bridge has several subscriptions on the channel.

Restart Aion Web after saving the configuration. The owner can select **Dots**
when starting a conversation and can open **Dots default conversation** from
the pinned section of the sidebar. Replies use the original conversation.
Independent messages from Dot appear in the default conversation. AionCore
continues receiving messages while the browser is closed.

Keep the same `url` and data directory when changing the relay transport so the
running AionCore can reuse the current inbox cursor and request associations.
With the workspace relay installed, run `./manage-services.py status dots-bridge`
from the Aion workspace root to check the local listener and the Bridge health.
The same target supports `start`, `stop`, and `restart`.

The first connection skips the existing Bridge inbox. Later connections use
the saved consumer position. Each Aion installation has its own consumer, so a
test installation does not move the production inbox position. Conversation
attachments are cached in the
corresponding temporary workspace and can be downloaded again while the
original remains in R2. The Bridge retains ownership of R2 credentials and
operators manage the bucket's retention.

To disable Dots, remove `dots-bridge.json` and restart. AionCore removes the
default conversation and its local workspace. Ordinary conversations retain
their history. Bridge connectivity problems and subscription expiry leave
the default conversation in place.
