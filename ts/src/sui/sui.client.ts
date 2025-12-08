// src/sui/sui.client.ts
import { SuiClient, getFullnodeUrl } from '@mysten/sui.js/client';

/**
 * Sui Client 的工厂函数，仅提供配置好的 SuiClient 实例。
 */
export function createSuiClient(rpcUrl: string): SuiClient {
    const client = new SuiClient({
        url: rpcUrl || getFullnodeUrl('mainnet'),
    });
    console.log(`✅ [Sui Client] Initialized for RPC`);
    return client;
}

// 导出 Client 的类型以便 Service 使用
export type SuiRpcClient = SuiClient;
