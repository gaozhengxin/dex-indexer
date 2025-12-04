## Run
历史数据
```shell
cargo run -- --remote-store-url https://checkpoints.mainnet.sui.io --first-checkpoint 110000000
cargo run -- --remote-store-url https://checkpoints.mainnet.sui.io
```
最近数据
```shell
cargo run -- --rpc-api-url https://fullnode.mainnet.sui.io:443 --first-checkpoint 218900000
cargo run -- --rpc-api-url http://192.168.0.142:9000 --skip-watermark
cargo run -- --rpc-api-url http://127.0.0.1:9000
```
