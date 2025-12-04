// ecosystem.config.js

// 假设项目根目录位于 /home/user/sui-indexer-worker
// 注意：你需要将这里的路径替换为你服务器上的实际绝对路径
const PROJECT_ROOT_PATH = '/path/to/your/sui-indexer-worker';

module.exports = {
    apps: [
        {
            name: "dex-indexer-worker",
            script: "node_modules/ts-node/dist/bin.js",
            args: "src/index.ts",
            exec_mode: "fork",
            instances: 1,
            autorestart: true,

            cwd: PROJECT_ROOT_PATH,

            env: {
                NODE_ENV: "production",
            },
        },
    ],
};