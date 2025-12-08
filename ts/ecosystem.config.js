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