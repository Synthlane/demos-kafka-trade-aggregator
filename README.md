# noida-demos
This repo contains any new demo project required for understanding of concepts. Never production use!!

Build a Live Trade Feed with Kafka

What you're building: You're going to tap into Binance's live market data and watch real Bitcoin, Ethereum, and Solana trades happen in real time — then build a pipeline that processes them. By the end of the week you'll have a system that tracks how much of each coin is being traded every minute, recovers from crashes on its own, and speeds up just by running one extra command.


Why Kafka: Every time someone buys or sells Bitcoin on Binance, an event fires. During busy market hours, thousands of these fire every second. A regular program reading them one by one would fall behind, lose data on a crash, and have no way to scale. Kafka solves all three problems — and this week you'll see exactly how, on real data, in a way that just reading docs never shows you.

The cool part: Binance makes their trade stream completely public. No account needed, no API key, nothing to sign up for. You'll connect to a live WebSocket URL and real trades will start pouring in immediately. The producer script that does this is already written for you — your job starts the moment data is inside Kafka.


