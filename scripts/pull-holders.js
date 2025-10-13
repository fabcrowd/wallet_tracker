#!/usr/bin/env node
'use strict';

const fs = require('fs');
const path = require('path');

const TOTAL_TEL_SUPPLY = 92_577_234_366;
const RETAIL_THRESHOLD = 0;
const MEGA_HOLDER_THRESHOLD = 20_000_000_000;
const DUNE_API_KEY = process.env.DUNE_API_KEY;
const DUNE_QUERY_ID = process.env.DUNE_QUERY_ID || '5949529';
const DUNE_BASE_URL = 'https://api.dune.com/api/v1';
const OUTPUT_PATH = path.resolve(__dirname, '..', 'public', 'data', 'tel-holders.json');
const CHAINS_CONFIG_PATH = path.resolve(__dirname, '..', 'config', 'chains.json');
const EXCLUSIONS_PATH = path.resolve(__dirname, '..', 'config', 'exclusions.json');

if (!DUNE_API_KEY) {
  console.error('Missing DUNE_API_KEY environment variable.');
  process.exit(1);
}

const chainsConfig = JSON.parse(fs.readFileSync(CHAINS_CONFIG_PATH, 'utf8'));
const exclusionSet = new Set(
  JSON.parse(fs.readFileSync(EXCLUSIONS_PATH, 'utf8')).map((addr) => addr.toLowerCase())
);

async function duneFetchJson(endpoint, init = {}) {
  const response = await fetch(`${DUNE_BASE_URL}${endpoint}`, {
    ...init,
    headers: {
      'Content-Type': 'application/json',
      'X-Dune-API-Key': DUNE_API_KEY,
      ...(init.headers || {})
    }
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`Dune API error (${response.status}): ${text}`);
  }

  return response.json();
}

async function executeQuery(queryId) {
  const data = await duneFetchJson(`/query/${queryId}/execute`, { method: 'POST' });
  if (!data.execution_id) {
    throw new Error('Dune response missing execution_id');
  }
  return data.execution_id;
}

async function waitForCompletion(executionId, { pollInterval = 5_000, timeout = 600_000 } = {}) {
  const start = Date.now();

  while (true) {
    const status = await duneFetchJson(`/execution/${executionId}/status`);
    const state = status.state;

    if (state === 'QUERY_STATE_COMPLETED') {
      return;
    }

    if (state === 'QUERY_STATE_FAILED' || state === 'QUERY_STATE_CANCELLED') {
      throw new Error(`Dune query failed: ${JSON.stringify(status)}`);
    }

    if (Date.now() - start > timeout) {
      throw new Error('Timed out waiting for Dune query to finish');
    }

    await new Promise((resolve) => setTimeout(resolve, pollInterval));
  }
}

async function fetchResults(executionId) {
  const rows = [];
  let offset = 0;
  const limit = 50_000;

  while (true) {
    const result = await duneFetchJson(
      `/execution/${executionId}/results?limit=${limit}&offset=${offset}`
    );

    const pageRows = result.result?.rows ?? [];
    rows.push(...pageRows);

    const nextOffset =
      result.next_offset ?? result.result?.next_offset ?? result.result?.metadata?.next_offset;

    if (nextOffset == null) {
      break;
    }

    offset = nextOffset;
  }

  return rows;
}

function normalizeAddress(value) {
  if (!value) return null;
  const raw = String(value);
  if (raw.startsWith('0x') || raw.startsWith('0X')) {
    return raw.toLowerCase();
  }
  if (raw.startsWith('\\x') || raw.startsWith('\\X')) {
    return `0x${raw.slice(2).toLowerCase()}`;
  }
  return `0x${raw.toLowerCase()}`;
}

function computeMetrics(holders, totalRetailSupply) {
  const metrics = {};
  const supply = totalRetailSupply || 0;
  const balances = holders.map((h) => h.balance);

  [10, 25, 50, 100].forEach((n) => {
    const top = holders.slice(0, n);
    const topBalance = top.reduce((sum, h) => sum + h.balance, 0);
    metrics[`top${n}`] = {
      count: Math.min(n, holders.length),
      balance: topBalance,
      percentage: supply > 0 ? ((topBalance / supply) * 100).toFixed(2) : '0.00'
    };
  });

  metrics.gini = calculateGiniCoefficient(balances);
  metrics.stdDev = calculateStdDeviation(balances);
  metrics.distribution = calculateDistribution(holders);

  return metrics;
}

function calculateGiniCoefficient(values) {
  if (!values.length) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const n = sorted.length;
  const totalSum = sorted.reduce((sum, value) => sum + value, 0);
  if (totalSum === 0) return 0;

  let cumulative = 0;
  for (let i = 0; i < n; i += 1) {
    cumulative += (n - i) * sorted[i];
  }

  return (n + 1 - (2 * cumulative) / totalSum) / n;
}

function calculateStdDeviation(values) {
  if (!values.length) return 0;
  const mean = values.reduce((sum, value) => sum + value, 0) / values.length;
  const variance =
    values.reduce((sum, value) => sum + (value - mean) ** 2, 0) / values.length;
  return Math.sqrt(variance);
}

function calculateDistribution(holders) {
  const brackets = [
    { min: 1_000, max: 10_000, label: '1K-10K' },
    { min: 10_000, max: 100_000, label: '10K-100K' },
    { min: 100_000, max: 1_000_000, label: '100K-1M' },
    { min: 1_000_000, max: 10_000_000, label: '1M-10M' },
    { min: 10_000_000, max: 100_000_000, label: '10M-100M' },
    { min: 100_000_000, max: Infinity, label: '100M+' }
  ];

  return brackets.map((bracket) => {
    const inBracket = holders.filter(
      (holder) => holder.balance >= bracket.min && holder.balance < bracket.max
    );
    return {
      label: bracket.label,
      count: inBracket.length,
      totalBalance: inBracket.reduce((sum, holder) => sum + holder.balance, 0)
    };
  });
}

function processChain(chainKey, rows) {
  const label = chainsConfig[chainKey]?.label ?? chainKey;
  const holders = [];
  const excludedAddresses = new Set();
  let excludedBalance = 0;
  let totalProcessed = 0;

  rows.forEach((row) => {
    const address = normalizeAddress(row.address);
    const balance = Number(row.telcoin_balance ?? row.amount ?? 0);

    if (!address || Number.isNaN(balance)) {
      return;
    }

    totalProcessed += balance;

    if (exclusionSet.has(address) || balance > MEGA_HOLDER_THRESHOLD) {
      excludedAddresses.add(address);
      excludedBalance += balance;
      return;
    }

    if (balance >= RETAIL_THRESHOLD) {
      holders.push({
        address,
        balance,
        chain: label
      });
    }
  });

  holders.sort((a, b) => b.balance - a.balance);
  const totalRetailSupply = holders.reduce((sum, holder) => sum + holder.balance, 0);

  return {
    key: chainKey,
    label,
    holders,
    totalRetailSupply,
    retailHolderCount: holders.length,
    excludedAddresses: Array.from(excludedAddresses),
    excludedBalances: { [chainKey]: excludedBalance },
    totalProcessedByChain: { [chainKey]: totalProcessed },
    metrics: computeMetrics(holders, totalRetailSupply)
  };
}

function buildCombinedDataset(chainResults) {
  const combinedHolders = [];
  const excludedAddresses = new Set();
  const excludedBalances = {};
  const totalProcessedByChain = {};
  let totalRetailSupply = 0;

  for (const [key, data] of Object.entries(chainResults)) {
    data.holders.forEach((holder) => {
      combinedHolders.push({ ...holder, chain: data.label });
    });

    totalRetailSupply += data.totalRetailSupply;

    Object.entries(data.excludedBalances || {}).forEach(([chainKey, value]) => {
      excludedBalances[chainKey] = (excludedBalances[chainKey] || 0) + value;
    });

    Object.entries(data.totalProcessedByChain || {}).forEach(([chainKey, value]) => {
      totalProcessedByChain[chainKey] = (totalProcessedByChain[chainKey] || 0) + value;
    });

    data.excludedAddresses.forEach((address) => excludedAddresses.add(address));
  }

  combinedHolders.sort((a, b) => b.balance - a.balance);

  return {
    key: 'combined',
    label: 'Combined',
    holders: combinedHolders,
    totalRetailSupply,
    retailHolderCount: combinedHolders.length,
    excludedAddresses: Array.from(excludedAddresses),
    excludedBalances,
    totalProcessedByChain,
    metrics: computeMetrics(combinedHolders, totalRetailSupply)
  };
}

async function main() {
  console.log(`Executing Dune query ${DUNE_QUERY_ID}...`);
  const executionId = await executeQuery(DUNE_QUERY_ID);
  await waitForCompletion(executionId);
  console.log('Fetching query results...');
  const rows = await fetchResults(executionId);

  if (!rows.length) {
    throw new Error('Dune query returned no rows.');
  }

  const rowsByChain = rows.reduce((acc, row) => {
    const key = (row.blockchain || row.chain || '').toLowerCase();
    if (!key) return acc;
    if (!acc[key]) acc[key] = [];
    acc[key].push(row);
    return acc;
  }, {});

  const chainResults = {};
  Object.keys(chainsConfig).forEach((chainKey) => {
    const chainRows = rowsByChain[chainKey] || [];
    chainResults[chainKey] = processChain(chainKey, chainRows);
  });

  const combined = buildCombinedDataset(chainResults);
  const payload = {
    generatedAt: new Date().toISOString(),
    thresholds: {
      retail: RETAIL_THRESHOLD,
      megaHolder: MEGA_HOLDER_THRESHOLD
    },
    totals: {
      totalSupply: TOTAL_TEL_SUPPLY
    },
    chains: chainResults,
    combined,
    chainOrder: Object.keys(chainsConfig)
  };

  const serialized = `${JSON.stringify(payload, null, 2)}\n`;
  let previous = null;
  try {
    previous = fs.readFileSync(OUTPUT_PATH, 'utf8');
  } catch (error) {
    if (error.code !== 'ENOENT') throw error;
  }

  if (previous === serialized) {
    console.log('No changes detected; data file remains unchanged.');
    return;
  }

  fs.mkdirSync(path.dirname(OUTPUT_PATH), { recursive: true });
  fs.writeFileSync(OUTPUT_PATH, serialized);
  console.log(`Updated holder snapshot -> ${OUTPUT_PATH}`);
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
