const PROJECT_ROOT_PATH = '/home/xdl/dex-indexer/ts';

module.exports = {
  apps: [
    {
      name: "dex-indexer-worker",
      script: "node_modules/ts-node/dist/bin.js",
      args: "--require dotenv/config src/index.ts",
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
