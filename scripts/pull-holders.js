#!/usr/bin/env node
'use strict';

const fs = require('fs');
const path = require('path');

const TOTAL_TEL_SUPPLY = 92_577_234_366;
const RETAIL_THRESHOLD = 1_000;
const MEGA_HOLDER_THRESHOLD = 20_000_000_000;
const API_KEY = process.env.MORALIS_API_KEY;
const API_BASE_URL = 'https://deep-index.moralis.io/api/v2.2/erc20';
const OUTPUT_PATH = path.resolve(__dirname, '..', 'public', 'data', 'tel-holders.json');
const CHAINS_CONFIG_PATH = path.resolve(__dirname, '..', 'config', 'chains.json');
const EXCLUSIONS_PATH = path.resolve(__dirname, '..', 'config', 'exclusions.json');

if (!API_KEY) {
  console.error('Missing MORALIS_API_KEY environment variable.');
  process.exit(1);
}

const chains = JSON.parse(fs.readFileSync(CHAINS_CONFIG_PATH, 'utf8'));
const exclusionSet = new Set(JSON.parse(fs.readFileSync(EXCLUSIONS_PATH, 'utf8')).map((addr) => addr.toLowerCase()));

async function fetchJson(url) {
  const response = await fetch(url, {
    headers: {
      'X-API-Key': API_KEY
    }
  });

  if (!response.ok) {
    const text = await response.text();
    throw new Error(`Request failed (${response.status}): ${text}`);
  }

  return response.json();
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

async function fetchChainHolders(chainKey, chainConfig) {
  const holders = [];
  const excludedAddresses = new Set();
  let excludedBalance = 0;
  let totalProcessed = 0;
  let cursor = null;

  console.log(`Fetching holders for ${chainConfig.label}...`);

  while (true) {
    const url = new URL(`${API_BASE_URL}/${chainConfig.contract}/owners`);
    url.searchParams.set('chain', chainConfig.chain);
    url.searchParams.set('limit', '100');
    if (cursor) {
      url.searchParams.set('cursor', cursor);
    }

    const data = await fetchJson(url);
    const results = data.result ?? [];
    if (!results.length) {
      break;
    }

    for (const owner of results) {
      const address = (owner.owner_address || owner.address || '').toLowerCase();
      const balance = parseFloat(owner.balance_formatted ?? owner.balance ?? '0');
      if (!address || Number.isNaN(balance)) continue;

      totalProcessed += balance;

      if (exclusionSet.has(address) || balance > MEGA_HOLDER_THRESHOLD) {
        excludedAddresses.add(address);
        excludedBalance += balance;
        continue;
      }

      if (balance >= RETAIL_THRESHOLD) {
        holders.push({
          address,
          balance,
          chain: chainConfig.label
        });
      }
    }

    const lastBalance = parseFloat(results.at(-1)?.balance_formatted ?? results.at(-1)?.balance ?? '0');
    cursor = data.cursor ?? null;

    if (!cursor) break;
    if (!Number.isNaN(lastBalance) && lastBalance < RETAIL_THRESHOLD) break;
  }

  holders.sort((a, b) => b.balance - a.balance);
  const totalRetailSupply = holders.reduce((sum, holder) => sum + holder.balance, 0);

  return {
    key: chainKey,
    label: chainConfig.label,
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
  const chainResults = {};

  for (const [chainKey, config] of Object.entries(chains)) {
    try {
      chainResults[chainKey] = await fetchChainHolders(chainKey, config);
    } catch (error) {
      console.error(`Failed to fetch data for ${config.label}: ${error.message}`);
      throw error;
    }
  }

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
    chainOrder: Object.keys(chains)
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
